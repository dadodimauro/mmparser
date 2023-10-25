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

parser.add_argument('--mode',
                    '-m',
                    type=str,
                    required=False,
                    default="append",
                    help="Specify the mode, can be 'append' to add to an existing csv file or 'new'"
                    )

parser.add_argument('--recursive',
                    '-r',
                    type=str2bool,
                    required=False,
                    default=True,
                    help="If the search for awr reports must be done recursively"
                    )

parser.add_argument('--tables',
                    type=str,
                    required=False,
                    default="tables.json",
                    help="Specify the path where to find the awr reports"
                    )

parser.add_argument('--inputDir',
                    type=str,
                    required=True,
                    help="Specify the path where to find the awr reports"
                    )

parser.add_argument('--outputDir',
                    type=str,
                    required=True,
                    help="Specify the path where to save the final .html report and .csv files"
                    )

args = parser.parse_args()
