#include "aggregate.h"

void aggregate_stats(slurmdb_stats_t *dest, slurmdb_stats_t *from)
{
    /* Means it is a blank record */
    if (from->cpu_min == NO_VAL)
        return;

    if (dest->vsize_max < from->vsize_max) {
        dest->vsize_max = from->vsize_max;
        dest->vsize_max_nodeid = from->vsize_max_nodeid;
        dest->vsize_max_taskid = from->vsize_max_taskid;
    }
    dest->vsize_ave += from->vsize_ave;

    if (dest->rss_max < from->rss_max) {
        dest->rss_max = from->rss_max;
        dest->rss_max_nodeid = from->rss_max_nodeid;
        dest->rss_max_taskid = from->rss_max_taskid;
    }
    dest->rss_ave += from->rss_ave;

    if (dest->pages_max < from->pages_max) {
        dest->pages_max = from->pages_max;
        dest->pages_max_nodeid = from->pages_max_nodeid;
        dest->pages_max_taskid = from->pages_max_taskid;
    }
    dest->pages_ave += from->pages_ave;

    if ((dest->cpu_min > from->cpu_min) || (dest->cpu_min == NO_VAL)) {
        dest->cpu_min = from->cpu_min;
        dest->cpu_min_nodeid = from->cpu_min_nodeid;
        dest->cpu_min_taskid = from->cpu_min_taskid;
    }
    dest->cpu_ave += from->cpu_ave;
    if ((from->consumed_energy == NO_VAL64) ||
        (dest->consumed_energy == NO_VAL64))
        dest->consumed_energy = NO_VAL64;
    else
        dest->consumed_energy += from->consumed_energy;
    dest->act_cpufreq += from->act_cpufreq;
    if (dest->disk_read_max < from->disk_read_max) {
        dest->disk_read_max = from->disk_read_max;
        dest->disk_read_max_nodeid = from->disk_read_max_nodeid;
        dest->disk_read_max_taskid = from->disk_read_max_taskid;
    }
    dest->disk_read_ave += from->disk_read_ave;
    if (dest->disk_write_max < from->disk_write_max) {
        dest->disk_write_max = from->disk_write_max;
        dest->disk_write_max_nodeid = from->disk_write_max_nodeid;
        dest->disk_write_max_taskid = from->disk_write_max_taskid;
    }
    dest->disk_write_ave += from->disk_write_ave;
}
