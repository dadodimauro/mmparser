import argparse


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


parser = argparse.ArgumentParser(description='AWR parser (html only)')

parser.add_argument('--instance',
                    '-i',
                    type=int,
                    required=False,
                    default=0,
                    help="Specify the instance to plot if AWR report for multiple instances" +
                         "are present in the input folder"
                    )

parser.add_argument('--type',
                    '-t',
                    type=str,
                    required=False,
                    nargs='*',  # 0 or more arguments
                    default=["png"],
                    help="Specify the format of the output images"
                    )

parser.add_argument('--parse',
                    type=str2bool,
                    required=False,
                    nargs='?',
                    const=True,
                    default=True,
                    help="set to False if only the plotting is needed"
                    )


args = parser.parse_args()
