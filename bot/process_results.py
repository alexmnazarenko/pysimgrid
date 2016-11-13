import argparse
import numpy as np
import scipy.stats as st


def process_results(results_file):
    results = {}
    with open(results_file, 'r') as f:
        while True:
            line = f.readline()
            if not line: break
            system = line.strip()
            workload = f.readline().strip()
            sched_result = {}
            while True:
                result = f.readline().strip()
                if result == '': break
                parts = result.split('\t')
                sched = parts[0]
                makespan = float(parts[1])
                sched_result[sched] = makespan
            for sched, makespan in sched_result.iteritems():
                if sched not in results:
                    results[sched] = {
                        'MS': [],
                        'NMS': []
                    }
                results[sched]['MS'].append(makespan)
                normalized_makespan = makespan / sched_result['OLB']
                results[sched]['NMS'].append(normalized_makespan)

    for sched, sched_results in results.iteritems():
        ms_results = np.array(sched_results['MS'])
        ms_mean = np.mean(ms_results)
        nms_results = np.array(sched_results['NMS'])
        if sched == 'OLB':
            nms_mean = np.mean(nms_results)
            nms_conf_low = nms_mean
            nms_conf_up = nms_mean
        else:
            nms_mean, nms_conf_low, nms_conf_up = mean_confidence_interval(nms_results)
        print('%20s\t%f\t%f [%f, %f]' % (sched, ms_mean, nms_mean, nms_conf_low, nms_conf_up))


def mean_confidence_interval(data, confidence=0.95):
    mean = np.mean(data)
    conf_int = st.t.interval(confidence, len(data) - 1, loc=mean, scale=st.sem(data))
    return mean, conf_int[0], conf_int[1]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process file with experiment results")
    parser.add_argument("file", type=str, help="input file")
    args = parser.parse_args()
    process_results(args.file)