"""Script for migrating the minimum necessary BAG 2.0 code to Pydro24_Dev"""


import shutil
import pathlib

APPLICATION_FOLDER = pathlib.Path(r'C:\Pydro24_Dev\NOAA\site-packages\Python3\svn_repo\HSTB\bag_converter')
SCRIPTS = pathlib.Path(__file__).parents[0]


def clear_folder(folder):
    """Recursively clear contents of a folder"""
    
    for path in folder.iterdir():
        if path.is_file():
            path.unlink()
        else:
            clear_folder(path)
    folder.rmdir()


def deploy_csf_to_pydro():
    # Minimum files needed
    # scripts/
    #   BAG_2.x_converter_qt_gui.py
    #   bag_processor.py
    # README.md

    clear_folder(APPLICATION_FOLDER)
    APPLICATION_FOLDER.mkdir()

    # scripts folder
    shutil.copy2(SCRIPTS / 'BAG_2.x_converter_qt_gui.py', APPLICATION_FOLDER / 'BAG_2.x_converter_qt_gui.py')
    shutil.copy2(SCRIPTS / 'bag_processor.py', APPLICATION_FOLDER / 'bag_processor.py')
    shutil.copy2(SCRIPTS.parents[0] / 'README.md', APPLICATION_FOLDER / 'README.md')
    # shutil.copy2(INPUTS / '__main__.py', REPO_FOLDER.parents[0] / '__main__.py')


if __name__ == "__main__":    
    deploy_csf_to_pydro()
    # increment_version()
    print('Done')