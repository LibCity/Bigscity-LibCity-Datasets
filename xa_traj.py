# link: Please contact the author!
from cd_traj import get_dynas


if __name__ == "__main__":
    filenames = ["xianshi_1001_1015", "xianshi_1015_1031",
                 "xianshi_1101_1105", "xianshi_1115_1130"]

    get_dynas(filenames, "xa_traj")
