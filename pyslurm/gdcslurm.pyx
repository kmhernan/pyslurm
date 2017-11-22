# cython: embedsignature=True
# cython: profile=False

import time as p_time
import os

from libc.stdint cimport uint8_t, uint16_t, uint32_t
from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport malloc, free

from cpython cimport bool

cdef extern from 'stdlib.h':
    ctypedef long long size_t

cdef extern from 'stdio.h':
    ctypedef struct FILE
    cdef FILE *stdout

cdef extern from 'Python.h':
    cdef FILE *PyFile_AsFile(object file)

cdef extern from 'time.h' nogil:
    ctypedef long time_t
    double difftime(time_t time1, time_t time2)
    time_t time(time_t *t)

cdef extern from "sys/wait.h" nogil:
    int WIFSIGNALED (int status)
    int WTERMSIG (int status)
    int WEXITSTATUS (int status)

try:
    import __builtin__
except ImportError:
    # Python 3
    import builtins as __builtin__

cimport slurm
cimport aggregate
include "bluegene.pxi"
include "slurm_defines.pxi"

#cdef inline IS_JOB_COMPLETE(slurm.slurm_job_info_t _X):
#    return (_X.job_state & JOB_STATE_BASE) == JOB_COMPLETE

#
# gdc account jobs
#
cdef class archive_job:
    u"""Class to access archived job info."""

    cdef:
        void *dbconn
        dict _JobDict
        slurm.List _JobList

    def __cinit__(self):
        self.dbconn = <void *>NULL
        self._JobDict = {}

    def __dealloc__(self):
        self.__destroy()

    cdef __destroy(self):
        u"""archive job Destructor method."""
        self._JobDict = {}

    def load(self, jobid):
        #jobid_s = str(jobid).encode("UTF-8")
        self.__load(str(jobid))

    cdef int __load(self, jobidp) except? -1:
        u"""Load db conn."""
        cdef:
            slurm.slurmdb_job_cond_t *cond = <slurm.slurmdb_job_cond_t *>slurm.xmalloc(sizeof(slurm.slurmdb_job_cond_t))
            slurm.List JobList = NULL
            char* jobid = jobidp
            int apiError = 0
            int numAdded = 0
            void* dbconn = slurm.slurmdb_connection_get()

        cond.step_list = slurm.slurm_list_create(slurm.slurmdb_destroy_selected_step) 
        numAdded = slurm.slurm_addto_step_list(cond.step_list, jobid)
        JobList = slurm.slurmdb_jobs_get(dbconn, cond)

        if JobList is NULL:
            apiError = slurm.slurm_get_errno()
            raise ValueError(slurm.stringOrNone(slurm.slurm_strerror(apiError), ''), apiError)
        else:
            self._JobList = JobList

        slurm.slurmdb_connection_close(&dbconn)
        slurm.slurmdb_destroy_selected_step(cond.step_list)
        return 0

    def get(self, jobid):
        self.__load(jobid)
        self.__get_record()
        return self._JobDict

    cdef __get_record(self):
        cdef:
            slurm.List job_list = NULL
            slurm.ListIterator jobIter = NULL
            int i = 0
            int jobNum = 0
            slurm.List step_list = NULL
            slurm.ListIterator stepIter = NULL
            int j = 0 
            int stepNum = 0
            dict J_dict = {}

        if self._JobList is not NULL:
            jobNum = slurm.slurm_list_count(self._JobList)
            jobIter = slurm.slurm_list_iterator_create(self._JobList)

            for i in range(jobNum): 
                job = <slurm.slurmdb_job_rec_t *>slurm.slurm_list_next(jobIter)
                name = slurm.stringOrNone(job.jobname, '')

                JobData = {}
                JobData[u"jobid"] = job.jobid
                JobData[u"jobname"] = job.jobname 
                JobData[u"associd"] = job.associd
                JobData[u"elapsed"] = job.elapsed
                JobData[u"exitcode"] = job.exitcode
                JobData[u"req_cpus"] = job.req_cpus
                JobData[u"req_gres"] = job.req_gres
                JobData[u"req_mem"] = job.req_mem
                JobData[u"show_full"] = job.show_full 
                JobData[u"state"] = job.state

                stepNum = slurm.slurm_list_count(job.steps)
                stepIter = slurm.slurm_list_iterator_create(job.steps)
                for j in range(stepNum):
                    step = <slurm.slurmdb_step_rec_t *>slurm.slurm_list_next(stepIter)
                    if step.state < JOB_COMPLETE:
                        continue
                    job.tot_cpu_sec += step.tot_cpu_sec
                    job.tot_cpu_usec += step.tot_cpu_usec
                    job.user_cpu_sec += step.user_cpu_sec
                    job.user_cpu_usec += step.user_cpu_usec
                    job.sys_cpu_sec += step.sys_cpu_sec
                    job.sys_cpu_usec += step.sys_cpu_usec 
                    aggregate.aggregate_stats(&job.stats, &step.stats)

                # stats
                JobData[u"tot_cpu_sec"] = job.tot_cpu_sec
                JobData[u"tot_cpu_usec"] = job.tot_cpu_usec
                JobData[u"user_cpu_sec"] = job.user_cpu_sec 
                JobData[u"user_cpu_usec"] = job.user_cpu_usec 
                JobData[u"sys_cpu_sec"] = job.sys_cpu_sec 
                JobData[u"sys_cpu_usec"] = job.sys_cpu_usec 
                JobData[u"vsize_max"] = job.stats.vsize_max
                JobData[u"vsize_ave"] = job.stats.vsize_ave
                JobData[u"consumed_energy"] = job.stats.consumed_energy

                J_dict[name] = JobData
            slurm.slurm_list_iterator_destroy(jobIter)
            slurm.slurm_list_destroy(self._JobList)
        self._JobDict = J_dict
