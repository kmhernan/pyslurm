from pyslurm.slurm cimport *

cdef extern from "aggregate.h":
    void aggregate_stats(slurmdb_stats_t *dest, slurmdb_stats_t *frm)
