#!/bin/bash
# cat run_scripts_1028.sh | while read i; do printf "%q\n" "$i"; done | xargs --max-procs=16 -I CMD bash -c CMD

# 01, Random, random, <none>, <none> @ openb_pod_list_default
EXPDIR="experiments/2025_1028/openb_pod_list_default/01-Random/1.3/42" && mkdir -p ${EXPDIR} && touch "${EXPDIR}/terminal.out" && python3 scripts/generate_config_and_run.py -d "${EXPDIR}" -e -b -f data/openb_pod_list_default -Random 1000 -gpusel random -tune 1.3 -tuneseed 42 --shuffle-pod=true -z "${EXPDIR}/snapshot/ds01" | tee -a "${EXPDIR}/terminal.out" && python3 scripts/analysis.py -f -g ${EXPDIR} | tee -a "${EXPDIR}/terminal.out" 

# 02, DotProd, best, merge, max @ openb_pod_list_default
EXPDIR="experiments/2025_1028/openb_pod_list_default/02-DotProd/1.3/42" && mkdir -p ${EXPDIR} && touch "${EXPDIR}/terminal.out" && python3 scripts/generate_config_and_run.py -d "${EXPDIR}" -e -b -f data/openb_pod_list_default -DotProd 1000 -gpusel best -dimext merge -norm max -tune 1.3 -tuneseed 42 --shuffle-pod=true -z "${EXPDIR}/snapshot/ds01" | tee -a "${EXPDIR}/terminal.out" && python3 scripts/analysis.py -f -g ${EXPDIR} | tee -a "${EXPDIR}/terminal.out" 

# 03, GpuClustering, <none>, <none>, <none> @ openb_pod_list_default
EXPDIR="experiments/2025_1028/openb_pod_list_default/03-GpuClustering/1.3/42" && mkdir -p ${EXPDIR} && touch "${EXPDIR}/terminal.out" && python3 scripts/generate_config_and_run.py -d "${EXPDIR}" -e -b -f data/openb_pod_list_default -GpuClustering 1000 -tune 1.3 -tuneseed 42 --shuffle-pod=true -z "${EXPDIR}/snapshot/ds01" | tee -a "${EXPDIR}/terminal.out" && python3 scripts/analysis.py -f -g ${EXPDIR} | tee -a "${EXPDIR}/terminal.out" 

# 04, GpuPacking, <none>, <none>, <none> @ openb_pod_list_default
EXPDIR="experiments/2025_1028/openb_pod_list_default/04-GpuPacking/1.3/42" && mkdir -p ${EXPDIR} && touch "${EXPDIR}/terminal.out" && python3 scripts/generate_config_and_run.py -d "${EXPDIR}" -e -b -f data/openb_pod_list_default -GpuPacking 1000 -tune 1.3 -tuneseed 42 --shuffle-pod=true -z "${EXPDIR}/snapshot/ds01" | tee -a "${EXPDIR}/terminal.out" && python3 scripts/analysis.py -f -g ${EXPDIR} | tee -a "${EXPDIR}/terminal.out" 

# 05, BestFit, <none>, <none>, <none> @ openb_pod_list_default
EXPDIR="experiments/2025_1028/openb_pod_list_default/05-BestFit/1.3/42" && mkdir -p ${EXPDIR} && touch "${EXPDIR}/terminal.out" && python3 scripts/generate_config_and_run.py -d "${EXPDIR}" -e -b -f data/openb_pod_list_default -BestFit 1000 -tune 1.3 -tuneseed 42 --shuffle-pod=true -z "${EXPDIR}/snapshot/ds01" | tee -a "${EXPDIR}/terminal.out" && python3 scripts/analysis.py -f -g ${EXPDIR} | tee -a "${EXPDIR}/terminal.out" 

# 06, FGD, FGD, share, max @ openb_pod_list_default
EXPDIR="experiments/2025_1028/openb_pod_list_default/06-FGD/1.3/42" && mkdir -p ${EXPDIR} && touch "${EXPDIR}/terminal.out" && python3 scripts/generate_config_and_run.py -d "${EXPDIR}" -e -b -f data/openb_pod_list_default -FGD 1000 -gpusel FGD -dimext share -norm max -tune 1.3 -tuneseed 42 --shuffle-pod=true -z "${EXPDIR}/snapshot/ds01" | tee -a "${EXPDIR}/terminal.out" && python3 scripts/analysis.py -f -g ${EXPDIR} | tee -a "${EXPDIR}/terminal.out" 

