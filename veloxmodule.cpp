#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "velox_common.h"

#include <velox/vector/arrow/Bridge.h>
#include <arrow/c/bridge.h>
#include <arrow/api.h>

extern "C"
{
    PyMODINIT_FUNC PyInit_velox(void);
}

static PyObject *PyArrowRecordBatchClass = NULL;

static PyObject *Velox_FromJson(PyObject *self, PyObject *arg);
static void      Velox_Dealloc(void *self);

static PyObject *VeloxResultIterator_Iter(PyObject *self);
static PyObject *VeloxResultIterator_Next(PyObject *self);
static void      VeloxResultIterator_Dealloc(PyObject *self);

static PyObject *VeloxVector_ToArrow(PyObject *self, PyObject *arg);
static PyObject *VeloxVector_Str(PyObject *self);
static PyObject *VeloxVector_Iter(PyObject *self);
static void      VeloxVector_Dealloc(PyObject *self);

static PyObject *VeloxVectorIterator_Next(PyObject *self);
static void      VeloxVectorIterator_Dealloc(PyObject *self);

static PyObject *VeloxRow_Str(PyObject *self);
static void      VeloxRow_Dealloc(PyObject *self);

static void      VeloxRecordBatch_Dealloc(PyObject *self);

static PyObject *VeloxError;

static PyMethodDef VeloxMethods[] =
{
    { "from_json", Velox_FromJson, METH_O },
    { NULL, NULL, 0, NULL },
};

static PyModuleDef VeloxModule =
{
    PyModuleDef_HEAD_INIT,
    "velox",
    "Run substrait plans using Velox",
    -1,
    VeloxMethods,
    .m_free = Velox_Dealloc,
};

struct VeloxResultIterator
{
    PyObject_HEAD;
    std::shared_ptr<facebook::velox::exec::Task> task;
};

static PyTypeObject VeloxResultIteratorType =
{
    PyObject_HEAD_INIT(NULL)
    .tp_name = "velox.ResultIterator",
    .tp_basicsize = sizeof(VeloxResultIterator),
    .tp_dealloc = VeloxResultIterator_Dealloc,
    .tp_doc = PyDoc_STR("Iterator over a Velox result"),
    .tp_iter = VeloxResultIterator_Iter,
    .tp_iternext = VeloxResultIterator_Next,
};

struct VeloxVector
{
    PyObject_HEAD;
    facebook::velox::RowVectorPtr vector;
    std::shared_ptr<facebook::velox::exec::Task> task;
};

static PyMethodDef VeloxVectorMethods[] =
{
    { "to_arrow", VeloxVector_ToArrow, METH_NOARGS, "Convert the result to a PyArrow array" },
    { NULL, NULL, 0, NULL },
};

static PyTypeObject VeloxVectorType =
{
    PyObject_HEAD_INIT(NULL)
    .tp_name = "velox.Vector",
    .tp_basicsize = sizeof(VeloxVector),
    .tp_dealloc = VeloxVector_Dealloc,
    .tp_str = VeloxVector_Str,
    .tp_doc = PyDoc_STR("Result of a Velox query"),
    .tp_iter = VeloxVector_Iter,
    .tp_methods = VeloxVectorMethods,
};

struct VeloxVectorIterator
{
    PyObject_HEAD;
    facebook::velox::vector_size_t row_idx;
    facebook::velox::RowVectorPtr vector;
    std::shared_ptr<facebook::velox::exec::Task> task;
};

static PyTypeObject VeloxVectorIteratorType =
{
    PyObject_HEAD_INIT(NULL)
    .tp_name = "velox.VectorIterator",
    .tp_basicsize = sizeof(VeloxVectorIterator),
    .tp_dealloc = VeloxVectorIterator_Dealloc,
    .tp_doc = PyDoc_STR("Iterator over a Velox Vector"),
    .tp_iternext = VeloxVectorIterator_Next,
};

struct VeloxRow
{
    PyObject_HEAD;
    facebook::velox::vector_size_t row_idx;
    facebook::velox::RowVectorPtr vector;
    std::shared_ptr<facebook::velox::exec::Task> task;
};

static PyTypeObject VeloxRowType =
{
    PyObject_HEAD_INIT(NULL)
    .tp_name = "velox.Row",
    .tp_basicsize = sizeof(VeloxRow),
    .tp_dealloc = VeloxRow_Dealloc,
    .tp_str = VeloxRow_Str,
    .tp_doc = PyDoc_STR("Object representing a Velox row"),
};

static PyTypeObject VeloxRecordBatchType =
{
    PyObject_HEAD_INIT(NULL)
    .tp_name = "velox.RecordBatch",
    .tp_dealloc = VeloxRecordBatch_Dealloc,
    .tp_doc = PyDoc_STR("Velox wrapper around an Arrow RecordBatch")
};

static int ImportPyArrowRecordBatch()
{
    if(!PyArrowRecordBatchClass)
    {
        struct AutoPyObject
        {
            PyObject *obj;
            AutoPyObject(PyObject *obj) : obj(obj) {}
            operator PyObject *() const { return obj; }
            operator bool() const { return obj; }
            ~AutoPyObject() { Py_XDECREF(obj); }
        };

        AutoPyObject pyarrow = PyImport_ImportModule("pyarrow.lib");
        if(!pyarrow)
            return -1;

        AutoPyObject dict = PyModule_GetDict(pyarrow);
        if(!dict)
            return -1;

        PyArrowRecordBatchClass = PyDict_GetItemString(dict, "RecordBatch");
        if(!PyArrowRecordBatchClass)
            return -1;
        Py_XINCREF(PyArrowRecordBatchClass);
    }
    PyTypeObject *arrow_rb_type = (PyTypeObject *)PyArrowRecordBatchClass;
    VeloxRecordBatchType = *arrow_rb_type;
    VeloxRecordBatchType.tp_basicsize += sizeof(std::shared_ptr<facebook::velox::exec::Task>);
    VeloxRecordBatchType.tp_base = arrow_rb_type;
    VeloxRecordBatchType.tp_flags |= Py_TPFLAGS_BASETYPE;
    VeloxRecordBatchType.tp_dealloc = VeloxRecordBatch_Dealloc;
    VeloxRecordBatchType.tp_name = "velox.RecordBatch";
    VeloxRecordBatchType.tp_doc = PyDoc_STR("Velox wrapper around an Arrow RecordBatch");
    return PyType_Ready(&VeloxRecordBatchType);
}

PyMODINIT_FUNC
PyInit_velox(void)
{
    PyObject *m;

    m = PyModule_Create(&VeloxModule);
    if (m == NULL)
        return NULL;

    VeloxError = PyErr_NewException("velox.error", NULL, NULL);
    Py_XINCREF(VeloxError);
    if(PyModule_AddObject(m, "error", VeloxError) < 0)
    {
        Py_XDECREF(VeloxError);
        Py_CLEAR(VeloxError);
        Py_DECREF(m);
        return NULL;
    }

    if(ImportPyArrowRecordBatch() < 0)
        return NULL;
    if(PyType_Ready(&VeloxResultIteratorType) < 0)
        return NULL;
    if(PyType_Ready(&VeloxVectorType) < 0)
        return NULL;
    if(PyType_Ready(&VeloxVectorIteratorType) < 0)
        return NULL;
    if(PyType_Ready(&VeloxRowType) < 0)
        return NULL;

    char arg[] = "PyVelox";
    char *argv_arr[] = { arg };
    char **argv = argv_arr;
    int argc = 1;
    try
    {
        InitVelox(&argc, &argv);
        return m;
    }
    catch(const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
        return NULL;
    }
}

static PyObject *Velox_FromJson(PyObject *self, PyObject *arg)
{
    std::string json = PyUnicode_AsUTF8(arg);
    if(PyErr_Occurred())
        return NULL;

    substrait::Plan substrait_plan;
    auto status = google::protobuf::util::JsonStringToMessage(json, &substrait_plan);
    if(!status.ok())
    {
        PyErr_SetString(VeloxError, std::string(status.message()).c_str());
        return NULL;
    }

    try
    {
        VeloxResultIterator *result_iterator = PyObject_New(VeloxResultIterator, &VeloxResultIteratorType);
        std::memset(&result_iterator->task, 0, sizeof(result_iterator->task));
        result_iterator->task = ExecuteSubstrait(substrait_plan);
        return (PyObject *)result_iterator;
    }
    catch(const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
        return NULL;
    }
}

static void Velox_Dealloc(void *)
{
    Py_XDECREF(PyArrowRecordBatchClass);
}

static PyObject *VeloxResultIterator_Iter(PyObject *self)
{
    return self;
}

static PyObject *VeloxResultIterator_Next(PyObject *self)
{
    VeloxResultIterator *_self = (VeloxResultIterator *)self;
    facebook::velox::RowVectorPtr next = _self->task->next();
    if(!next)
    {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    VeloxVector *result = PyObject_New(VeloxVector, &VeloxVectorType);
    std::memset(&result->vector, 0, sizeof(result->vector));
    std::memset(&result->task, 0, sizeof(result->task));
    result->vector = std::move(next);
    result->task = _self->task;
    return (PyObject *)result;
}

static void VeloxResultIterator_Dealloc(PyObject *self)
{
    VeloxResultIterator *_self = (VeloxResultIterator *)self;
    _self->task.reset();
}

static PyObject *VeloxVector_ToArrow(PyObject *self, PyObject *args)
{
    VeloxVector *_self = (VeloxVector *)self;
    ArrowArray arrow_array;
    ArrowSchema arrow_schema;
    try
    {
        facebook::velox::exportToArrow(_self->vector, arrow_schema);
        facebook::velox::exportToArrow(_self->vector, arrow_array, _self->vector->pool());
        _self->vector.reset();
    }
    catch(const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
        return NULL;
    }
    PyObject *arrow_rb = PyObject_CallMethod(PyArrowRecordBatchClass, "_import_from_c", "KK", (uintptr_t)&arrow_array, (uintptr_t)&arrow_schema);
    PyObject *velox_rb = _PyObject_GC_New(&VeloxRecordBatchType);
    std::memset(velox_rb + 1, 0, VeloxRecordBatchType.tp_basicsize - sizeof(PyObject));
    std::memcpy(velox_rb + 1, arrow_rb + 1, ((PyTypeObject *)PyArrowRecordBatchClass)->tp_basicsize - sizeof(PyObject));
    PyObject_GC_UnTrack(arrow_rb);
    PyObject_GC_Del(arrow_rb);
    arrow_rb->ob_refcnt = 0;
    uint8_t *task_ptr = (((uint8_t *)velox_rb) + VeloxRecordBatchType.tp_basicsize - sizeof(std::shared_ptr<facebook::velox::exec::Task>));
    *(std::shared_ptr<facebook::velox::exec::Task> *)task_ptr = _self->task;
    return velox_rb;
}

static PyObject *VeloxVector_Str(PyObject *self)
{
    VeloxVector *_self = (VeloxVector *)self;
    if(!_self->vector)
    {
        PyErr_SetString(VeloxError, "Empty vector! It may have been moved into an Arrow recordbatch");
        return NULL;
    }
    std::string str = _self->vector->toString();
    return PyUnicode_FromString(str.c_str());
}

static PyObject *VeloxVector_Iter(PyObject *self)
{
    VeloxVector *_self = (VeloxVector *)self;
    VeloxVectorIterator *iter = PyObject_New(VeloxVectorIterator, &VeloxVectorIteratorType);
    std::memset(&iter->vector, 0, sizeof(iter->vector));
    std::memset(&iter->task, 0, sizeof(iter->task));
    iter->row_idx = 0;
    iter->vector = _self->vector;
    iter->task = _self->task;
    return (PyObject *)iter;
}

static void VeloxVector_Dealloc(PyObject *self)
{
    VeloxVector *_self = (VeloxVector *)self;
    try
    {
        _self->vector.reset();
        _self->task.reset();
    }
    catch(const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
    }
}

static PyObject *VeloxVectorIterator_Next(PyObject *self)
{
    VeloxVectorIterator *_self = (VeloxVectorIterator *)self;
    if(_self->row_idx == _self->vector->size())
    {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    VeloxRow *next = PyObject_New(VeloxRow, &VeloxRowType);
    std::memset(&next->vector, 0, sizeof(next->vector));
    std::memset(&next->task, 0, sizeof(next->task));
    next->row_idx = _self->row_idx++;
    next->vector = _self->vector;
    next->task = _self->task;
    return (PyObject *)next;
}

static void VeloxVectorIterator_Dealloc(PyObject *self)
{
    VeloxVectorIterator *_self = (VeloxVectorIterator *)self;
    try
    {
        _self->vector.reset();
        _self->task.reset();
    }
    catch(const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
    }
}

static PyObject *VeloxRow_Str(PyObject *self)
{
    VeloxRow *_self = (VeloxRow *)self;
    std::string str = _self->vector->toString(_self->row_idx);
    return PyUnicode_FromString(str.c_str());
}

static void VeloxRow_Dealloc(PyObject *self)
{
    VeloxRow *_self = (VeloxRow *)self;
    try
    {
        _self->vector.reset();
        _self->task.reset();
    }
    catch(const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
    }
}

static void VeloxRecordBatch_Dealloc(PyObject *self)
{
    ((PyTypeObject *)PyArrowRecordBatchClass)->tp_dealloc(self);
    uint8_t *task_ptr = (((uint8_t *)self) + VeloxRecordBatchType.tp_basicsize - sizeof(std::shared_ptr<facebook::velox::exec::Task>));
    try
    {
        ((std::shared_ptr<facebook::velox::exec::Task> *)task_ptr)->reset();
    }
    catch(const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
    }

}
