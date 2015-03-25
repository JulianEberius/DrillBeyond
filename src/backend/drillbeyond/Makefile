#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for drillbeyond
#
# IDENTIFICATION
#    src/backend/drillbeyond/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/drillbeyond
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS= drillbeyond_rewriting.o drillbeyond_operator.o drillbeyond_cost.o \
	  drillbeyond_requests.o drillbeyond_explain.o drillbeyond_sampling.o \
	  drillbeyond_planner.o drillbeyond_hashtable.o drillbeyond_compress.o \
	  drillbeyond_debug.o drillbeyond_reoptimization.o

include $(top_srcdir)/src/backend/common.mk
