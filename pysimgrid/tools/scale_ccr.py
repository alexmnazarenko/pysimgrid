import argparse


def calculate_ccr(file):
    total_comm = 0.
    total_comp = 0.
    with open(file, "r") as input:
        for line in input:
            if '->' in line:
                size = float(line.split('size="')[1].split('"')[0])
                total_comm += size
            elif 'size="' in line:
                size = float(line.split('size="')[1].split('"')[0])
                total_comp += size

    return total_comm / total_comp


def main():
    parser = argparse.ArgumentParser(description="Scale CCR to desired value")
    parser.add_argument("input_file", type=str, help="path to input DOT file")
    parser.add_argument("-c", "--ccr", type=float, help="target CCR value")
    parser.add_argument("-s", "--speed", type=float, help="host speed in Gflop/s")
    parser.add_argument("-b", "--bandwidth", type=float, help="network bandwidth in MBps")
    parser.add_argument("-o", "--output-file", type=str, help="path to output DOT file")

    args = parser.parse_args()

    input_ccr = calculate_ccr(args.input_file)
    print("Input CCR: %f" % input_ccr)
    if args.speed is not None and args.bandwidth is not None:
        system_factor = (args.speed * 1e9) / (args.bandwidth * 1e6)
        input_ccr *= system_factor
        print("Input system CCR: %f" % input_ccr)
    else:
        system_factor = None

    if args.output_file is not None:
        factor = args.ccr / input_ccr
        with open(args.output_file, "w") as out:
            with open(args.input_file, "r") as input:
                for line in input:
                    if '->' in line:
                        parts = line.split('size="')
                        size = float(parts[1].split('"')[0])
                        new_size = size * factor
                        new_line = '%ssize="%e"%s' % (parts[0], new_size, parts[1].split('"')[1])
                        out.write(new_line)
                    else:
                        out.write(line)

        output_ccr = calculate_ccr(args.output_file)
        print("Output CCR: %f" % output_ccr)
        if system_factor is not None:
            print("Output system CCR: %f" % (output_ccr * system_factor))


if __name__ == "__main__":
    main()
