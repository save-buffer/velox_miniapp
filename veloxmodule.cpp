#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "velox_common.h"

extern "C"
{
    PyMODINIT_FUNC PyInit_velox(void);
    static PyObject *velox_from_json(PyObject *self, PyObject *arg);
}

static PyObject *VeloxError;

static PyMethodDef VeloxMethods[] =
{
    { "from_json", velox_from_json, METH_O },
    { NULL, NULL, 0, NULL },
};

static struct PyModuleDef VeloxModule =
{
    PyModuleDef_HEAD_INIT,
    "velox",
    "Run substrait plans using Velox",
    -1,
    VeloxMethods
};

extern "C"
{
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

        char arg[] = "PyVelox";
        char *argv_arr[] = { arg };
        char **argv = argv_arr;
        int argc = 1;
        InitVelox(&argc, &argv);
        return m;
    }

    static PyObject *velox_from_json(PyObject *self, PyObject *arg)
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
            auto task = ExecuteSubstrait(substrait_plan);
            while(auto result = task->next())
            {
                std::cout << "Got a result of size " << result->size() << ":\n";
                for(auto i = 0; i < result->size(); i++)
                    std::cout << "\t" << result->toString(i) << "\n";
            }
            return self;
        }
        catch (const facebook::velox::VeloxException &e)
        {
            PyErr_SetString(VeloxError, e.message().c_str());
            return NULL;
        }
    }
}

