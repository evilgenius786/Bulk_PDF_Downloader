import csv
import datetime
import os
import threading
import time
import traceback

import requests

thread_count = 10
semaphore = threading.Semaphore(thread_count)
write = threading.Lock()
incsv = "PDF_Links.csv"
headers = ["UNIQUE LINKS", "TITLE", "TITLE CLEAN (: https:@@ with _, / with @)"]
checkedcsv = "checked1.csv"
downloaded = os.listdir()
checkedheaders = ["URL", "Title", "Result"]
checked = []


def download(url, filename, line):
    with semaphore:
        if filename not in downloaded:
            try:
                pprint(f"Downloading {filename} from {url}")
                with open("./files/" + filename, "wb") as outfile:
                    outfile.write(requests.get(url).content)
                downloaded.append(filename)
                pprint(f"Downloaded {filename}")
                if filename not in checked:
                    data = {
                        "Title": filename,
                        "URL": line["UNIQUE LINKS"]
                    }
                    with open(f"./files/{filename}", encoding='utf8', errors='ignore') as tmpfile:
                        if tmpfile.read().startswith("%PDF"):
                            data['Result'] = "OK"
                        else:
                            data['Result'] = "Error"
                    with write:
                        with open(checkedcsv, 'a', newline='') as asdfile:
                            csv.DictWriter(asdfile, fieldnames=checkedheaders).writerow(data)
                    print(data)
                    checked.append(filename)
            except Exception as ex:
                traceback.print_exc()
                pprint(f"Error on  {filename} from {url}")
                with write:
                    with open("Error.csv", 'a', newline='', encoding='utf8') as efile:
                        csv.writer(efile).writerow([url, filename, ex])
        else:
            pprint(f"Already downloaded {filename}")


def main():
    logo()
    threads = []
    files = os.listdir('./files')
    if os.path.isfile(checkedcsv):
        with open(checkedcsv) as cfile:
            for line in csv.DictReader(cfile, fieldnames=checkedheaders):
                checked.append(line['Title'])
    else:
        with open(checkedcsv, 'w') as cfile:
            csv.DictWriter(cfile, fieldnames=checkedheaders).writeheader()
    with open(incsv) as cfile:
        x = csv.DictReader(cfile, fieldnames=headers)
        next(x)  # skip header
        for line in x:
            title = line["TITLE CLEAN (: https:@@ with _, / with @)"]
            filename = title.replace("?", "@")
            if filename not in files:
                thread = threading.Thread(target=download, args=(line["UNIQUE LINKS"], filename, line))
                thread.start()
                threads.append(thread)
                time.sleep(0.1)
            else:
                # pprint(f"Already downloaded {filename}")
                pass
    for thread in threads:
        thread.join()


def pprint(msg):
    print(str(datetime.datetime.now()).split(".")[0], msg)


def logo():
    os.system('color 0a')
    os.system('cls')
    print(fr"""
     ___  ___   ___   ___                      _                _           
    | _ \|   \ | __| |   \  ___ __ __ __ _ _  | | ___  __ _  __| | ___  _ _ 
    |  _/| |) || _|  | |) |/ _ \\ V  V /| ' \ | |/ _ \/ _` |/ _` |/ -_)| '_|
    |_|  |___/ |_|   |___/ \___/ \_/\_/ |_||_||_|\___/\__,_|\__,_|\___||_|  
===================================================================================
           Bulk PDF downloader by https://github.com/evilgenius786
===================================================================================
[+] Multithreaded (Thread count: {thread_count})
[+] Super fast and efficient
[+] Proper logging
[+] Exception handling
[+] Reading data from CSV {incsv}
___________________________________________________________________________________
""")


if __name__ == '__main__':
    main()
