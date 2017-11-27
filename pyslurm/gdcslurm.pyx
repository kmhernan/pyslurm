# cython: embedsignature=True
# cython: profile=False
import time as p_time
import os
from pyslurm import convSecs2time_str, get_job_state

from libc.string cimport strlen, strcpy
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
        list _JobDictList
        slurm.List _JobList

    def __cinit__(self):
        self.dbconn       = <void *>NULL
        self._JobList     = NULL
        self._JobDictList = [] 

    def __dealloc__(self):
        self.__destroy()

    cdef __destroy(self):
        u"""archive job Destructor method."""
        self._JobDictList = []

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
        #slurmdb_destroy_job_cond
        #slurmdb_destroy_job_rec
        slurm.slurmdb_connection_close(&dbconn)
        slurm.slurmdb_destroy_selected_step(cond.step_list)
        slurm.slurmdb_destroy_job_cond(cond)
        #slurm.xfree(cond)
        return 0

    def get(self, jobid):
        self.load(jobid)
        self.__get_record()
        return self._JobDictList

    cpdef str __get_time_str(self, slurm.time_t timerec):
        cdef:
            int sz = 32
            char * tmp_str = <char *>slurm.xmalloc(<size_t>sz)
        slurm.slurm_make_time_str(&timerec, tmp_str, sz)
        res = tmp_str.encode("UTF-8") 
        slurm.xfree(tmp_str)
        return res 

    cdef __get_record(self):
        cdef:
            slurm.List job_list = NULL
            slurm.ListIterator jobIter = NULL
            int i = 0
            int jobNum = 0
            int exit_code = slurm.NO_VAL
            slurm.List step_list = NULL
            slurm.ListIterator stepIter = NULL
            int j = 0 
            int stepNum = 0
            list J_list = [] 
           
        if self._JobList is not NULL:
            jobNum = slurm.slurm_list_count(self._JobList)
            jobIter = slurm.slurm_list_iterator_create(self._JobList)

            for i in range(jobNum): 
                job = <slurm.slurmdb_job_rec_t *>slurm.slurm_list_next(jobIter)
                name = slurm.stringOrNone(job.jobname, '')
                JobData = {}

                if name:
                    # Get the number of steps
                    stepNum = slurm.slurm_list_count(job.steps)

                    if stepNum:
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

                        # Destroy
                        slurm.slurm_list_iterator_destroy(stepIter)

                    # stats
                    JobData[u"jobid"]    = job.jobid
                    JobData[u"jobname"]  = slurm.stringOrNone(job.jobname, '')
                    JobData[u"exitcode"] = '' if job.state < JOB_COMPLETE \
                        else slurm.int32orNone(job.exitcode)
                            
                    #JobData[u"exitcode"] = slurm.int32orNone(job.exitcode)
                    JobData[u"state"]    = get_job_state(job.state)

                    # Requested resources
                    ## At this time I get some segfaults with the int64 types
                    #JobData[u"req_cpus"] = slurm.int32orNone(job.req_cpus)
                    #JobData[u"req_gres"] = slurm.stringOrNone(job.req_gres, '')
                    #JobData[u"req_mem"]  = slurm.int64orNone(job.req_mem)
                    #JobData[u"tres_alloc_str"] = slurm.stringOrNone(job.tres_alloc_str, '')
                    #JobData[u"tres_req_str"]   = slurm.stringOrNone(job.tres_req_str, '')

                    # Time info
                    #elapsed = convSecs2time_str(job.elapsed)
                    #JobData[u"elapsed"] = elapsed 

                    #JobData[u"submitted"] = self.__get_time_str(job.submit) 
                    #JobData[u"start"]   = self.__get_time_str(job.start) 
                    
                    #if tmp_str is not NULL:
                    #    JobData[u"start"] = tmp_str
                    #    slurm.xfree(tmp_str)
                    #else:
                    #    JobData[u"start"] = ""
                    #slurm.slurm_make_time_str(&job.end, tmp_str, sz)
                    #JobData[u"end"]       = tmp_str 
                    #JobData[u"tot_cpu_sec"]     = slurm.int32orNone(job.tot_cpu_sec)
                    #JobData[u"tot_cpu_usec"]    = slurm.int32orNone(job.tot_cpu_usec)
                    #JobData[u"user_cpu_sec"]    = slurm.int32orNone(job.user_cpu_sec)
                    #JobData[u"user_cpu_usec"]   = slurm.int32orNone(job.user_cpu_usec)
                    #JobData[u"sys_cpu_sec"]     = slurm.int32orNone(job.sys_cpu_sec) 
                    #JobData[u"sys_cpu_usec"]    = slurm.int32orNone(job.sys_cpu_usec)
                    #JobData[u"vsize_max"]       = job.stats.vsize_max / 1024
                    #JobData[u"vsize_ave"]       = slurm.int64orNone(<uint64_t>job.stats.vsize_ave)
                    #JobData[u"vsize_ave"]       = job.stats.vsize_ave / 1024
                    #JobData[u"consumed_energy"] = job.stats.consumed_energy
                    #JobData[u"act_cpufreq"]     = job.stats.act_cpufreq
                    #JobData[u"cpu_ave"]         = job.stats.cpu_ave
                    #JobData[u"cpu_min"]         = job.stats.cpu_min
                    #JobData[u"disk_read_ave"]   = job.stats.disk_read_ave
                    #JobData[u"disk_write_ave"]  = job.stats.disk_write_ave

                if name:
                    J_list.append(JobData)

            slurm.slurm_list_iterator_destroy(jobIter)
            slurm.slurm_list_destroy(self._JobList)

        self._JobDictList = J_list

cdef class BatchJob:
    u"""Class for submitting Batch jobs"""
    cdef:
        dict response_msg

    def __cinit__(self):
        self.response_msg = {}

    def __dealloc__(self):
        self.__destroy()

    cdef __destroy(self):
        u"""archive job Destructor method."""
        self.response_msg = {}

    def submit(self, requirements):
        #self.__load_reqs(requirements) 
        self.__submit(requirements)
        return self.response_msg

    #cpdef void __load_reqs(self, dict requirements):
    #    slurm.slurm_init_job_desc_msg( &self.job_desc_msg )
    #    if requirements.get('max_nodes'):
    #        self.job_desc_msg.max_nodes = requirements[u'max_nodes']
    #    if requirements.get('min_nodes'):
    #        self.job_desc_msg.min_nodes = requirements[u'min_nodes']    
    #    if requirements.get('name'):
    #        self.job_desc_msg.name = requirements[u'name']
    #    if requirements.get('work_dir'):
    #        self.job_desc_msg.work_dir = requirements[u'work_dir']
    #    if requirements.get('cpus_per_task'):
    #        self.job_desc_msg.cpus_per_task = requirements[u'cpus_per_task']
    #    if requirements.get('partition'):
    #        self.job_desc_msg.partition = requirements[u'partition']
    #    if requirements.get('mem'):
    #        self.job_desc_msg.pn_min_memory = <int64_t>requirements[u'mem']
    #    self.job_desc_msg.script = requirements[u'script']

    cpdef void __submit(self, dict requirements):
        cdef:
            slurm.job_desc_msg_t job_desc_msg
            slurm.submit_response_msg_t * slurm_alloc_msg
            int ret = -1 
            int apiError = -1

        # Initialize message
        slurm.slurm_init_job_desc_msg( &job_desc_msg )

        # Add requirements
        if requirements.get('max_nodes'):
            job_desc_msg.max_nodes = requirements[u'max_nodes']
        if requirements.get('min_nodes'):
            job_desc_msg.min_nodes = requirements[u'min_nodes']    
        if requirements.get('name'):
            name = requirements[u'name'].encode("UTF-8", "replace")
            job_desc_msg.name = name 
        #if requirements.get('work_dir'):
        #    work_dir = requirements[u'work_dir'].encode("UTF-8", "replace")
        #    job_desc_msg.work_dir = work_dir 
        work_dir = os.cwd.encode("UTF-8", "replace")
        job_desc_msg.work_dir = work_dir
        if requirements.get('cpus_per_task'):
            job_desc_msg.cpus_per_task = requirements[u'cpus_per_task']
        if requirements.get('partition'):
            partition = requirements[u'partition'].encode("UTF-8", "replace")
            job_desc_msg.partition = partition 
        if requirements.get('mem'):
            job_desc_msg.pn_min_memory = <int64_t>requirements[u'mem']
        if requirements.get('user_id'):
            job_desc_msg.user_id = <uint32_t>requirements[u'user_id'] 
        if requirements.get('group_id'):
            job_desc_msg.group_id = <uint32_t>requirements[u'group_id'] 
        if requirements.get('ntasks'):
            job_desc_msg.num_tasks = <uint32_t>requirements[u'ntasks'] 

        script = requirements[u'script'].encode("UTF-8", "replace")
        job_desc_msg.script = script 

        # Submit job 
        ret = slurm.slurm_submit_batch_job(&job_desc_msg, &slurm_alloc_msg)
        if ret != 0:
            apiError = slurm.slurm_get_errno()
            raise ValueError(slurm.stringOrNone(slurm.slurm_strerror(apiError), ''), apiError)
        else:
            self.response_msg[u'job_id'] = slurm_alloc_msg.job_id
            self.response_msg[u'step_id'] = slurm_alloc_msg.step_id
            self.response_msg[u'error_code'] = slurm_alloc_msg.error_code

        slurm.slurm_free_submit_response_response_msg( slurm_alloc_msg )
