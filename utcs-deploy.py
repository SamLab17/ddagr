import subprocess
import tempfile
import os
import stat
from time import sleep
from bs4 import BeautifulSoup
import urllib.request
from subprocess import call
import sys
import ssl

user = "slaberge"
suffix = ".cs.utexas.edu"
nHosts = 5

SCP = "/usr/bin/scp"
SSH = "/usr/bin/ssh"
SBT = "/Users/sam/Library/Application Support/Coursier/bin/sbt"

GOOD_EXECUTORS = [
    "ens31np",
    "brittle-star",
    "fichte",
    "crinoid",
    "cancer",
    "marx",
    "magenta",
    "locke",
    "king-of-tokyo",
    "hi-ho-cherry-o",
    "descartes",
    "bodhidharma",
    "leibniz",
    "hegel",
    "camelopardalis",
    "cyan",
    "dolpopa",
    "delphinus",
    "clank",
    "canis-major",
    "bourdieu",
    "equuleus",
    "epicurus",
    "dorado",
    "chess",
    "aristotle",
    "hayek"
]

SKIP_HOSTS=[
    "apple-jacks",
    "crazy-cow",
    "count-chocula",
    "cookie-crisp",
    "chocapic",
    "frosted-mini-wheats",
    "capn-crunch",
    "cracklin-oat-bran",
    "cocoa-puffs",
    "freakies",
    "froot-loops",
    "crispix",
    "corn-flakes",
    "grape-nuts",
    "fruit-brute",
    "golden-grahams",
    "fruity-pebbles",
    "green-slime",
    "honey-ohs",
    "honey-monster-puffs",
    "honey-bunches-of-oats",
]


schedulerJar = "scheduler-assembly-0.1.0-SNAPSHOT.jar"
executorJar = "executor-assembly-0.1.0-SNAPSHOT.jar"

# scheduler = "frosted-mini-wheats"

def assemble():
    print("Assembling jar files...")
    proc = subprocess.run([SBT, "scheduler/assembly; executor/assembly"], 
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        print(proc.stdout.decode("utf-8"))
        print(proc.stderr.decode("utf-8"))
        exit(1)

def transfer():
    print("Transferring jar files...")
    jars = ["scheduler/target/scala-2.13/" + schedulerJar,
            "executor/target/scala-2.13/" + executorJar]
    for jar in jars:
        proc = subprocess.run([SCP, "-q", jar, user + "@linux" + suffix + ":~"])
        if proc.returncode != 0:
            print(f"Failed to upload {jar}")
            exit(1)

def sshRun(machine, command):
    with tempfile.NamedTemporaryFile(mode='w+t', suffix=".command", delete=False) as tmp:
        tmp.writelines([
            "#!/bin/sh\n",
            SSH + " -t -t -oStrictHostKeyChecking=no " + user + "@" + machine + suffix + " \"" + command + "\"\n"
        ])
        st = os.stat(tmp.name)
        os.chmod(tmp.name, st.st_mode | stat.S_IEXEC)
        sleep(0.5)
        return subprocess.Popen(["/usr/bin/open", "-a", "Terminal.app", tmp.name]) 
    # os.system(ssh_command)

def execute(hosts):
    scheduler = hosts[0]
    executors = hosts[1:]

    separator = "*"*16
    print(f"{separator}\nSCHEDULER: {scheduler}\n{separator}")

    scheduler_command = "java -Xmx8g -jar " + schedulerJar
    executor_command = "SCHEDULER="+ scheduler + " java -Xmx8g -jar " + executorJar

    # Run scheduler and give it a few seconds to fully start up
    sshRun(scheduler, scheduler_command)
    sleep(5.0)

    toRun = [(e, executor_command) for e in executors]
    procs = [sshRun(m, c) for (m, c) in toRun]

def getHosts(n):
    res = []
    for i in range(min(n, len(GOOD_EXECUTORS))):
        res.append(GOOD_EXECUTORS[i])
    return res

def findHosts(n):
    url = 'https://apps.cs.utexas.edu/unixlabstatus/'

    try:
        # Create an SSL context to connect to UTCS's site
        context = ssl._create_unverified_context()
        # Read in website as a string
        site = urllib.request.urlopen(url, context=context).read()
    except:
        print("Could not connect to UTCS Unix hosts site")
        sys.exit(1)

    #Create Web Scraper instance
    soup = BeautifulSoup(site, 'html.parser')

    #Create a list of all the hosts on the site
    hosts = []

    # Loop through all hosts on website
    for host in soup.find_all('tr'):
        #Get the text for this item
        text = host.get_text()	
        #Split up text into an array of lines
        lines = text.splitlines();	

        # Valid hosts take up 6 lines
        if len(lines) == 6:
            #Get the name of the host 
            name = lines[1]
            # Can be either 'up' or 'down'
            status = lines[2]	
            # Make sure this isn't the table header and that the host is 'up'
            if name != 'Host' and status == 'up':	
                load = float(lines[5])
                users = int(float(lines[4]))
                hosts.append((name, users, load))
    # Sort by load, then users
    hosts.sort(key=lambda tup: (tup[2], tup[1]))
    hosts = [h for h in hosts if h[0] not in SKIP_HOSTS]
    result = []
    for i in range(n):
        result.append(hosts[i][0])
    print(f"Found hosts: {result}")
    return result

def main():

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('numExec', type=int)
    parser.add_argument('--scheduler', default=None)
    parser.add_argument('--fast', action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <num hosts>")
        exit(1)

    if not args.fast:
        assemble()
        transfer()

    # hosts = findHosts(args.numExec)
    hosts = getHosts(args.numExec)
    if args.scheduler:
        hosts = [args.scheduler] + hosts

    execute(hosts)

if __name__ == "__main__":
    main()
