#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <dlfcn.h>

#include "velox_common.h"

#include <velox/vector/arrow/Bridge.h>
#include <arrow/c/bridge.h>

namespace arrow::py
{
    PyObject *wrap_batch(const std::shared_ptr<arrow::RecordBatch> &batch);
    int import_pyarrow();
}

extern "C"
{
    PyMODINIT_FUNC PyInit_velox(void);
}

static bool PyArrowImported = false;

static PyObject *Velox_FromJson(PyObject *self, PyObject *arg);

static PyObject *VeloxResultIterator_Iter(PyObject *self);
static PyObject *VeloxResultIterator_Next(PyObject *self);
static void      VeloxResultIterator_Dealloc(PyObject *self);

static PyObject *VeloxResult_ToArrow(PyObject *self, PyObject *arg);
static PyObject *VeloxResult_Str(PyObject *self);
static PyObject *VeloxResult_Iter(PyObject *self);
static PyObject *VeloxResult_Next(PyObject *self);
static void      VeloxResult_Dealloc(PyObject *self);

static PyObject *VeloxRow_Str(PyObject *self);
static void      VeloxRow_Dealloc(PyObject *self);

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
    VeloxMethods
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

struct VeloxResult
{
    PyObject_HEAD;
    facebook::velox::vector_size_t row_idx;
    facebook::velox::RowVectorPtr result;
    std::shared_ptr<facebook::velox::exec::Task> task;
};

static PyMethodDef VeloxResultMethods[] =
{
    { "to_arrow", VeloxResult_ToArrow, METH_NOARGS, "Convert the result to a PyArrow array" },
    { NULL, NULL, 0, NULL },
};

static PyTypeObject VeloxResultType =
{
    PyObject_HEAD_INIT(NULL)
    .tp_name = "velox.Result",
    .tp_basicsize = sizeof(VeloxResult),
    .tp_dealloc = VeloxResult_Dealloc,
    .tp_str = VeloxResult_Str,
    .tp_doc = PyDoc_STR("Result of a Velox query"),
    .tp_iter = VeloxResult_Iter,
    .tp_iternext = VeloxResult_Next,
    .tp_methods = VeloxResultMethods,
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

    if(PyType_Ready(&VeloxResultIteratorType) < 0)
        return NULL;
    if(PyType_Ready(&VeloxResultType) < 0)
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
    catch (const facebook::velox::VeloxException &e)
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
    catch (const facebook::velox::VeloxException &e)
    {
        PyErr_SetString(VeloxError, e.message().c_str());
        return NULL;
    }
}

static PyObject *VeloxResultIterator_Iter(PyObject *self)
{
    Py_INCREF(self);
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

    VeloxResult *result = PyObject_New(VeloxResult, &VeloxResultType);
    std::memset(&result->result, 0, sizeof(result->result));
    std::memset(&result->task, 0, sizeof(result->task));
    result->result = std::move(next);
    result->task = _self->task;
    return (PyObject *)result;
}

static void VeloxResultIterator_Dealloc(PyObject *self)
{
    VeloxResultIterator *_self = (VeloxResultIterator *)self;
    _self->task.reset();
}

static PyObject *VeloxResult_ToArrow(PyObject *self, PyObject *args)
{
    if(!PyArrowImported)
    {
        if(arrow::py::import_pyarrow() < 0)
            return NULL;
        PyArrowImported = true;
    }
    VeloxResult *_self = (VeloxResult *)self;
    ArrowArray arrow_array;
    ArrowSchema arrow_schema;
    facebook::velox::exportToArrow(_self->result, arrow_array);
    facebook::velox::exportToArrow(_self->result, arrow_schema);
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_rb = arrow::ImportRecordBatch(&arrow_array, &arrow_schema);
    if(!maybe_rb.ok())
    {
        PyErr_SetString(VeloxError, maybe_rb.status().message().c_str());
        return NULL;
    }
    std::shared_ptr<arrow::RecordBatch> rb = maybe_rb.MoveValueUnsafe();
    return arrow::py::wrap_batch(rb);
}

static PyObject *VeloxResult_Str(PyObject *self)
{
    VeloxResult *_self = (VeloxResult *)self;
    std::string str = _self->result->toString();
    return PyUnicode_FromString(str.c_str());
}

static PyObject *VeloxResult_Iter(PyObject *self)
{
    VeloxResult *_self = (VeloxResult *)self;
    _self->row_idx = 0;
    return self;
}

static PyObject *VeloxResult_Next(PyObject *self)
{
    VeloxResult *_self = (VeloxResult *)self;
    if(_self->row_idx == _self->result->size())
    {
        PyErr_SetNone(PyExc_StopIteration);
        return NULL;
    }

    VeloxRow *next = PyObject_New(VeloxRow, &VeloxRowType);
    std::memset(&next->vector, 0, sizeof(next->vector));
    std::memset(&next->task, 0, sizeof(next->task));
    next->row_idx = _self->row_idx++;
    next->vector = _self->result;
    next->task = _self->task;
    return (PyObject *)next;
}

static void VeloxResult_Dealloc(PyObject *self)
{
    VeloxResult *_self = (VeloxResult *)self;
    _self->result.reset();
    _self->task.reset();
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
    _self->vector.reset();
    _self->task.reset();
}
