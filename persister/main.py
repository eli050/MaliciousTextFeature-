from db_maneger import ManagerPypline



if __name__ == '__main__':
    manager = ManagerPypline()
    result = manager.run_pipeline()
    print(result)
