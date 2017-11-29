# cython: embedsignature=True
# cython: profile=False
import time as p_time
import os
import sys
from socket import gethostname
from pyslurm import convSecs2time_str, get_job_state

from libc.string cimport strlen, strcpy
from libc.stdint cimport uint8_t, uint16_t, uint32_t
from libc.stdint cimport int64_t, uint64_t
from libc.stdlib cimport malloc, free
from posix.unistd cimport getuid, getgid

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

cdef extern from "<sys/resource.h>" nogil:
    enum: PRIO_PROCESS
    int getpriority(int, id_t)

try:
    import __builtin__
except ImportError:
    # Python 3
    import builtins as __builtin__

cimport slurm
cimport aggregate
include "bluegene.pxi"
include "slurm_defines.pxi"

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
            #char* jobid = jobidp
            int apiError = 0
            int numAdded = 0
            void* dbconn = slurm.slurmdb_connection_get()

        if not cond.step_list:
            cond.step_list = slurm.slurm_list_create(slurm.slurmdb_destroy_selected_step) 
        numAdded = slurm.slurm_addto_step_list(cond.step_list, <char *>jobidp)
        if not slurm.slurm_list_count(cond.step_list):
            slurm.slurm_list_destroy(cond.step_list)
            cond.step_list = NULL

        JobList = slurm.slurmdb_jobs_get(dbconn, cond)
        if JobList is NULL:
            apiError = slurm.slurm_get_errno()
            raise ValueError(slurm.stringOrNone(slurm.slurm_strerror(apiError), ''), apiError)
        else:
            self._JobList = JobList

        #slurm.slurm_list_destroy(JobList)
        #JobList = NULL
        slurm.slurmdb_connection_close(&dbconn)
        #slurm.slurmdb_destroy_selected_step(cond.step_list)
        slurm.slurmdb_destroy_job_cond(cond)
        return 0

    def get(self, jobid):
        self.load(jobid)
        self.__get_record()
        return self._JobDictList

    cpdef unicode __get_time_str(self, slurm.time_t timerec):
        cdef:
            int sz = 32
            char * tmp_str = <char *>slurm.xmalloc(<size_t>sz)
        slurm.slurm_make_time_str(&timerec, tmp_str, sz)
        #res = tmp_str.encode("UTF-8") 
        res = slurm.stringOrNone(tmp_str, '')
        slurm.xfree(tmp_str)
        return res

    cdef void __get_record(self):
        cdef:
            slurm.List job_list = NULL
            slurm.ListIterator jobIter = NULL
            int i = 0
            int jobNum = 0
            uint16_t exit_status
            uint16_t term_sig
            slurm.List step_list = NULL
            slurm.ListIterator stepIter = NULL
            int j = 0 
            int stepNum = 0
            list J_list = [] 

        exit_status = 0
        term_sig = 0

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

                    if WIFSIGNALED(job.derived_ec):
                        term_sig = WTERMSIG(job.derived_ec)
                    else:
                        term_sig = 0

                    exit_status = WEXITSTATUS(job.derived_ec)
                    JobData[u"derived_ec"] = str(exit_status) + ":" + str(term_sig)

                    if WIFSIGNALED(job.exitcode):
                        term_sig = WTERMSIG(job.exitcode)
                    exit_status = WEXITSTATUS(job.exitcode)
                    
                    JobData[u"exitcode"] = str(exit_status) + ":" + str(term_sig) 
                    JobData[u"state"]    = get_job_state(job.state)

                    # Requested resources
                    JobData[u"req_cpus"] = slurm.int32orNone(job.req_cpus)
                    JobData[u"req_mem"]  = slurm.int64orNone(job.req_mem)

                    # Time info
                    JobData[u"elapsed"]   = slurm.stringOrNone(convSecs2time_str(job.elapsed), '')
                    JobData[u"submitted"] = self.__get_time_str(job.submit)
                    JobData[u"start"]     = self.__get_time_str(job.start)
                    JobData[u"end"]       = self.__get_time_str(job.end) 

                    # Usage
                    JobData[u"total_cpu"]     = _elapsed_time(job.tot_cpu_sec, job.tot_cpu_usec)
                    JobData[u"user_cpu"]      = _elapsed_time(job.user_cpu_sec, job.user_cpu_usec)
                    JobData[u"system_cpu"]    = _elapsed_time(job.sys_cpu_sec, job.sys_cpu_usec)

                    if job.stats.vsize_max != <uint64_t>slurm.NO_VAL64:
                        JobData[u"vsize_max"] = job.stats.vsize_max
                    else:
                        JobData[u"vsize_max"] = None

                    if <double>job.stats.vsize_ave != slurm.NO_VAL:
                        JobData[u"vsize_ave"] = <double>job.stats.vsize_ave
                    else:
                        JobData[u"vsize_ave"] = None

                    ##if job.stats.consumed_energy != NO_VAL_DOUBLE:
                    ##    JobData[u"consumed_energy"] = job.stats.consumed_energy
                    ##else:
                    ##    JobData[u"consumed_energy"] = None 
                    #
                    if job.stats.act_cpufreq != slurm.NO_VAL:
                        JobData[u"act_cpufreq"] = <double>job.stats.act_cpufreq / 1000
                    else:
                        JobData[u"act_cpufreq"] = None 

                    if job.stats.cpu_ave != slurm.NO_VAL: 
                        JobData[u"cpu_ave"] = _elapsed_time(<long>job.stats.cpu_ave, 0L)
                    else:
                        JobData[u"cpu_ave"] = None 

                    if job.stats.cpu_min != slurm.NO_VAL:
                        JobData[u"cpu_min"] = _elapsed_time(<long>job.stats.cpu_min, 0L)
                    else:
                        JobData[u"cpu_min"] = None

                    JobData[u"disk_read_ave"] = None if job.stats.disk_read_ave == slurm.NO_VAL \
                        else job.stats.disk_read_ave
                    JobData[u"disk_write_ave"] = None if job.stats.disk_write_ave == slurm.NO_VAL \
                        else job.stats.disk_write_ave

                    #(uint64_t)job->elapsed
                    #* (uint64_t)cpu_tres_rec_count;
                    JobData[u"nodes"] = slurm.stringOrNone(job.nodes, '')

                if name:
                    J_list.append(JobData)

            slurm.slurm_list_iterator_destroy(jobIter)
            slurm.slurm_list_destroy(self._JobList)
            self._JobList = NULL

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

    cdef int envcount(self, char **env):
        """Return the number of elements in the environment `env`."""
        cdef int envc = 0
        while (env[envc] != NULL):
            envc += 1
        return envc

    cpdef void __submit(self, dict requirements):
        cdef:
            slurm.job_desc_msg_t job_desc_msg
            slurm.submit_response_msg_t * slurm_alloc_msg
            int ret = -1 
            int apiError = -1

        errno = 0
        retval = 0
        retval = getpriority(PRIO_PROCESS, 0)
        if retval == -1:
            if errno:
                raise ValueError("getpriority(PRIO_PROCESS): %m")
        try:
            os.environ["SLURM_PRIO_PROCESS"] = str(retval)
        except:
            raise ValueError("unable to set SLURM_PRIO_PROCESS in environment")

        try:
            os.environ["SLURM_SUBMIT_DIR"] = os.getcwd()
        except:
            raise ValueError("unable to set SLURM_SUBMIT_DIR in environment")

        try:
            os.environ["SLURM_SUBMIT_HOST"] = gethostname()
        except:
            raise ValueError("unable to set SLURM_SUBMIT_HOST in environment")

        if not os.environ.get("SLURM_UMASK"):
            mask = os.umask(0)
            _ = os.umask(mask)
            try:
                os.environ["SLURM_UMASK"] = "0" + str((mask>>6)&07) + str((mask>>3)&07) + str(mask&07)
            except:
                raise ValueError("unable to set SLURM_UMASK in environment")

        # Initialize message
        slurm.slurm_init_job_desc_msg( &job_desc_msg )

        # Add requirements
        if requirements.get("contiguous") == 1:
            job_desc_msg.contiguous = 1
        else: job_desc_msg.contiguous = 0
        if requirements.get('max_nodes'):
            job_desc_msg.max_nodes = requirements[u'max_nodes']
        if requirements.get('min_nodes'):
            job_desc_msg.min_nodes = requirements[u'min_nodes']    
        if requirements.get('name'):
            name = requirements[u'name'].encode("UTF-8", "replace")
            job_desc_msg.name = name 
            os.environ["SLURM_JOB_NAME"] = name
        else:
            job_desc_msg.name = "sbatch"
            os.environ["SLURM_JOB_NAME"] = "sbatch" 

        if requirements.get("work_dir"):
            workdir = requirements[u'work_dir'].encode("UTF-8", "replace")
            job_desc_msg.work_dir = workdir 
        else:
            work_dir = os.getcwd().encode("UTF-8", "replace")
            job_desc_msg.work_dir = work_dir

        if requirements.get('cpus_per_task'):
            job_desc_msg.cpus_per_task = requirements[u'cpus_per_task']
            os.environ["SLURM_CPUS_PER_TASK"] = str(requirements[u'cpus_per_task'])
        if requirements.get('partition'):
            partition = requirements[u'partition'].encode("UTF-8", "replace")
            job_desc_msg.partition = partition 
        if requirements.get('mem'):
            job_desc_msg.pn_min_memory = requirements[u'mem']
        if requirements.get('user_id'):
            job_desc_msg.user_id = requirements[u'user_id'] 
        else:
            job_desc_msg.user_id = getuid()
 
        if requirements.get('group_id'):
            job_desc_msg.group_id = requirements[u'group_id'] 
        else:
            job_desc_msg.group_id = getgid()
 
        if requirements.get('ntasks'):
            job_desc_msg.num_tasks = requirements[u'ntasks'] 
            os.environ["SLURM_NPROCS"] = str(requirements[u'ntasks'])
            os.environ["SLURM_NTASKS"] = str(requirements[u'ntasks'])

        script = requirements[u'script'].encode("UTF-8", "replace")
        job_desc_msg.script = script
        job_desc_msg.immediate = 0
        job_desc_msg.profile = ACCT_GATHER_PROFILE_NOT_SET
        job_desc_msg.mem_bind_type = 0
        job_desc_msg.task_dist = slurm.SLURM_DIST_UNKNOWN
        job_desc_msg.mail_type = 0
        job_desc_msg.begin_time = 0
        job_desc_msg.deadline = 0
        job_desc_msg.environment = NULL
        job_desc_msg.std_in = "/dev/null"
        job_desc_msg.ckpt_dir = slurm.slurm_get_checkpoint_dir()
        job_desc_msg.ckpt_interval = <uint16_t>0

        requirements[u"get_user_env_time"] = -1
        if not requirements.get("export_env"):
            slurm.slurm_env_array_merge(&job_desc_msg.environment, <char **>slurm.environ)

        if requirements[u"get_user_env_time"] >= 0:
            slurm.slurm_env_array_overwrite(&job_desc_msg.environment, "SLURM_GET_USER_ENV", "1")

        job_desc_msg.env_size = self.envcount(job_desc_msg.environment)
        #if slurm.slurm_job_will_run(&job_desc_msg) != slurm.SLURM_SUCCESS:
        #    slurm.slurm_perror("allocation failure")
        #    sys.exit(1)
        #sys.exit(0)

        # Submit job 
        ret = slurm.slurm_submit_batch_job(&job_desc_msg, &slurm_alloc_msg)
        if ret != 0:
            apiError = slurm.slurm_get_errno()
            raise ValueError(slurm.stringOrNone(slurm.slurm_strerror(apiError), ''), apiError)
        else:
            self.response_msg[u'job_id'] = slurm_alloc_msg.job_id
            #self.response_msg[u'step_id'] = slurm_alloc_msg.step_id
            self.response_msg[u'error_code'] = slurm_alloc_msg.error_code

        slurm.slurm_free_submit_response_response_msg( slurm_alloc_msg )

cdef _elapsed_time(long secs, long usecs):
    cdef:
        long days, hours, minutes, seconds
        long subsec = 0

    if secs < 0 or secs == <long>slurm.NO_VAL:
        return None 

    while usecs >= 1E6:
        secs += 1
        usecs -= <long>1E6
    
    if (usecs > 0):
        subsec = usecs / 1000

    seconds =  secs % 60
    minutes = (secs / 60)   % 60
    hours   = (secs / 3600) % 24
    days    =  secs / 86400

    if days:
        return u"%ld-%2.2ld:%2.2ld:%2.2ld" % (days, hours,
                                              minutes, seconds)
    elif hours:
        return u"%2.2ld:%2.2ld:%2.2ld" % (hours,
                                          minutes, seconds)
    elif subsec:
        return u"%2.2ld:%2.2ld.%3.3ld" % (minutes, seconds, subsec)
    else:
        return u"00:%2.2ld:%2.2ld" % (minutes, seconds)
