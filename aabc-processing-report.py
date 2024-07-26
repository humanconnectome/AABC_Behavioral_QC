import requests, json
from base64 import encodebytes
from getpass import getpass

# read sessions included in data freeze
included_sessions_file = open('aabc-nov23-data-freeze.txt')
included_sessions = included_sessions_file.read()
included_sessions_file.close()

included_sessions = included_sessions.split('\n')
#included_sessions = [ "_".join((x, 'MR')) for x in included_sessions ] # Append "_MR" suffix

# get JSESSION
server="https://hcpi-shadow14.nrg.wustl.edu/"

print("Username:", end=" ")
username = input()
password = getpass()

userPass = username + ":" + password
base64string = encodebytes(('%s:%s' % (username, password)).encode()).decode().strip()

# Set up session (this will keep the JSESSION alive for future requests)
session = requests.Session()

# Get JSESSION
session.get(server + 'data/JSESSION', headers={"Authorization": "Basic %s" % base64string})

projects = ['AABC_STG', 'AABC_MGH_ITK', 'AABC_UCLA_ITK', 'AABC_UMN_ITK', 'AABC_WU_ITK']

for project in projects:
    project_PCP = session.get(server + 'xapi/pipelineControlPanel/project/' + project + '/statusSummary')
    pcp = json.loads(project_PCP.content)
    
    #print(json.dumps(pcp, indent=2))

    project_Pipelines = []
    [project_Pipelines.append(x["pipeline"]) for x in pcp if x["pipeline"] not in project_Pipelines]
    
    #print(project_Pipelines)
    
    if project == 'AABC_STG':
        project_Pipelines.remove('AutoReclean')
        project_Pipelines.remove('TaskAnalysis')
        project_Pipelines.remove('TaskPerformancePipeline')
        project_Pipelines.remove('Tica')
    else:
        project_Pipelines.remove('DCM2NII')

    #print(project_Pipelines)

    #project_Pipelines = ['SanityChecks']

    print(project)

    #print("Pipeline,Unrunnable,Prereqs Unmet,Unvalidated,Reset,Error,Issues,Unknown,Completed,Total,Completed %")
    print("Pipeline,Unrunnable,Prereqs Unmet,Unvalidated,Issues,Completed,Total,Completed %")

    for pipe in project_Pipelines:
        pipe_PCP = session.get(server + 'xapi/pipelineControlPanel/project/' + project + '/pipeline/' + pipe + '/status')
        
        pipelist = json.loads(pipe_PCP.content)
        #print(json.dumps(pipelist, indent=2))
        
        print(pipe, end=",")
        #print("Total: " + str(len(pipelist)))
        
        step1list = []
        
        if pipe == 'SessionBuilding':
            #print(pipelist[0]["entityLabel"].rsplit("_", 1))]
            #print(pipelist[0]["entityLabel"] + "_" + pipelist[0]["subGroup"])
            step1list = [ x for x in pipelist if (x["entityLabel"] + "_" + x["subGroup"]) in included_sessions]
        else:
            #print(pipelist[0]["entityLabel"].rsplit("_", 1))
            step1list = [ x for x in pipelist if x["entityLabel"].rsplit("_", 1)[0] in included_sessions]
        
        # Print list of structural sessions for Erin
        #if pipe == 'StructuralPreprocessing':
        #    print([x["entityLabel"] for x in step1list])
        
        #print("Freeze: " + str(len(step1list)))
           
        unrunnable = []
        step2list = []
        for x in step1list:
            if x["impeded"] == True:
                unrunnable.append(x)
            else:
                step2list.append(x)
                
        #print("Unrunnable: " + str(len(unrunnable)))
        print(str(len(unrunnable)), end=",")
        #print("Remaining: " + str(len(step3list)))
        
        prereqsunmet = []
        step3list = []
        for x in step2list:
            if x["prereqs"] == False:
                prereqsunmet.append(x)
            else:
                step3list.append(x)
        #print("Prereqs Unmet: " + str(len(prereqsunmet)))
        print(str(len(prereqsunmet)), end=",")
        
        #print("Remaining: " + str(len(step2list)))
        
        unvalidated = []
        step4list = []
        for x in step3list:
            if x["validated"] == False:
                unvalidated.append(x)
                #dfpipelist.remove(x)
            else:
                step4list.append(x)
        #print("Unvalidated: " + str(len(unvalidated)))
        print(str(len(unvalidated)), end=",")
        
        #print("Remaining: " + str(len(step5list)))
        
        # reset = []
        # step5list = []
        # for x in step4list:
            # if x["status"] == "RESET":
                # reset.append(x)
                # #dfpipelist.remove(x)
            # else:
                # step5list.append(x)
        # #print("Completed: " + str(len(reset)))
        # print(str(len(reset)), end=",")
        
        # error = []
        # step6list = []
        # for x in step5list:
            # if x["status"] == "ERROR":
                # error.append(x)
                # #dfpipelist.remove(x)
            # else:
                # step6list.append(x)
        # #print("Completed: " + str(len(error)))
        # print(str(len(error)), end=",")
        
        step6list = step4list
            
        issues = []
        step7list = []
        for x in step6list:
            if x["issues"] == True:
                issues.append(x)
            else:
                step7list.append(x)
                
        #print("Issues: " + str(len(issues)))
        print(str(len(issues)), end=",")
        
        #print("Remaining: " + str(len(step4list)))
        
        # unknown = []
        # step8list = []
        # for x in step7list:
            # if x["status"] == "UNKNOWN":
                # unknown.append(x)
            # else:
                # step8list.append(x)
                
        # #print("Unknown: " + str(len(unknown)))
        # print(str(len(unknown)), end=",")
        
        #print("Remaining: " + str(len(step4list)))
        
        step8list = step7list
        
        successful = []
        step9list = []
        for x in step8list:
            #if (x["status"] == "COMPLETE" or x["status"] == "EXT_COMPLETE"):
            if (x["status"] in ("COMPLETE", "EXT_COMPLETE", "RESET", "ERROR", "UNKNOWN", "RUNNING")):
                successful.append(x)
                #dfpipelist.remove(x)
            else:
                step9list.append(x)
        #print("Completed: " + str(len(successful)))
        print(str(len(successful)), end=",")

        
        # Total # data freeze subjects minus unrunnable
        total_less_unrunnable = len(step1list) - len(unrunnable)
        print(str(total_less_unrunnable), end=",")
        
        #completed = round((len(successful) / total_less_unrunnable) * 100, 2)
        completed = round((len(successful) / total_less_unrunnable) * 100)
        #print("Completed %: " + str(completed))
        print(str(completed))
        
        #print(json.dumps(dfpipelist, indent=2))
        
        unique_pipestatus = [x["status"] for x in step8list]
        counts = {x: unique_pipestatus.count(x) for x in unique_pipestatus}
        #print(counts)
        
        #print()
    print()