import sys
import os
from datetime import datetime, timedelta


def file_exists(filepath):
    return os.path.exists(filepath)


def file_modified(filepath, return_as_date=False):
    if not file_exists(filepath):
        return None

    modified_time = os.path.getmtime(filepath)
    created_time = os.path.getctime(filepath)

    if return_as_date:
        return {"Modified": datetime.fromtimestamp(modified_time), "Created": datetime.fromtimestamp(created_time)}
    else:
        return {"Modified": modified_time, "Created": created_time}


def days_from_creation(filename):
    return (datetime.now() - file_modified(filename,True)["Modified"]).days


if __name__ == "__main__":
    file_to_analyze = sys.argv[0]
    # file_to_analyze = r"C:\Users\Gorelov\Desktop\DNS Parser\pyZip\price-sochi.zip"

    print(file_to_analyze, file_exists(file_to_analyze), file_modified(file_to_analyze), days_from_creation(file_to_analyze))
    print(file_to_analyze, file_exists(file_to_analyze), file_modified(file_to_analyze, True), days_from_creation(file_to_analyze))



'''
print("Modified")
print(os.stat(file)[-2])
print(os.stat(file).st_mtime)
print(os.path.getmtime(file))

print()

print("Created")
print(os.stat(file)[-1])
print(os.stat(file).st_ctime)
print(os.path.getctime(file))

print()

modified = os.path.getmtime(file)
print("Date modified: "+time.ctime(modified))
print("Date modified:",datetime.datetime.fromtimestamp(modified))
year,month,day,hour,minute,second=time.localtime(modified)[:-3]
print("Date modified: %02d/%02d/%d %02d:%02d:%02d"%(day,month,year,hour,minute,second))

print()

created = os.path.getctime(file)
print("Date created: "+time.ctime(created))
print("Date created:",datetime.datetime.fromtimestamp(created))
year,month,day,hour,minute,second=time.localtime(created)[:-3]
print("Date created: %02d/%02d/%d %02d:%02d:%02d"%(day,month,year,hour,minute,second))
'''
