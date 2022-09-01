"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import os
from datetime import timedelta
#imports do codigo anterior ObjectSearch.py
import requests
import json
import sys
import csv
import datetime
import inspect

def retrieve_name(var):
    print('[retrieve_name] inicio')
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return ''.join(map(str,[var_name for var_name, var_val in callers_local_vars if var_val is var]))

def get_session_id(username, password):
    print("[get_session_id] inicio: " + " usuario: " + username + "; password: " + password)
    session_id = ''
    data = {'@type': 'connin', 'username': username, 'password': password}
    url = "https://dm-us2.informaticacloud.com/ma/api/v2/user/login"
    # url = "https://dm-us.informaticacloud.com/saas/public/core/v3/login"
    # headers = {'Content-Type':'application/json', 'Accept':'application/json'}
    headers = {'Content-Type': 'application/json'}
    r = requests.post(url, data=json.dumps(data), headers=headers)

    print('[get_session_id]Codigo status API: ' + str(r.status_code))

    if r.status_code == 200:
        session_id = r.json()["icSessionId"]
        server_url = r.json()["serverUrl"]

    else:
        print('[get_session_id]Falha na chamada da API:')
        print(r.headers)
        print(r.json())
        sys.exit(1)
        print('[get_session_id] fim')
    return session_id, server_url

def get_activityconn(session_id, server_url):
    print("[get_activityconn]inicio")
    job_start_url = server_url + "/api/v2/activity/activityconn"
    headers = {'Content-Type': 'application/json', 'icSessionId': session_id, 'Accept': 'application/json'}
    data = {}
    r = requests.get(job_start_url, data=json.dumps(data), headers=headers)
    print("[get_activityconn]fim")
    return r.json()

def getWorkflows(session_id, server_url):
    print("[getWorkflows]inicio")
    job_start_url = server_url + "/api/v2/workflow"
    headers = {'Content-Type': 'application/json', 'icSessionId': session_id, 'Accept': 'application/json'}
    data = {}
    r = requests.get(job_start_url, data=json.dumps(data), headers=headers)
    print("[getWorkflows]fim")
    return r.json()

def SearchMetadataMCT(session_id, server_url, idMCT):
    print("[SearchMetadataMCT]inicio")
    job_start_url = server_url + "/api/v2/mttask/" + idMCT
    print(job_start_url)
    headers = {'Content-Type': 'application/json', 'icSessionId': session_id, 'Accept': 'application/json'}
    data = {}
    r = requests.get(job_start_url, data=json.dumps(data), headers=headers)
    print("[SearchMetadataMCT]fim")
    return r.json()

def get_cnn_detail(session_id, server_url, connId):
    #print("[get_cnn_detail]inicio")
    job_start_url = server_url + "/api/v2/connection/" + connId
    headers = {'Content-Type': 'application/json', 'icSessionId': session_id, 'Accept': 'application/json'}
    data = {}
    r = requests.get(job_start_url, data=json.dumps(data), headers=headers)
    #print("[get_cnn_detail]fim")
    return r.json()

def criarArquivo(caminhoAbsoluto, arquivoOutput):
    print('[criarArquivo]inicio')
    myFile = open(arquivoOutput, 'w', newline='')
    spamwriter = csv.writer(myFile, delimiter='|', quotechar=',',quoting=csv.QUOTE_MINIMAL)
    print('[criarArquivo]fim. Arquivo criado')
    return myFile, spamwriter

def fechaArquivo(myFile):
    print('[fechaArquivo]inicio')
    myFile.close()

def getComTratamento(objeto, nomeCampo):
    #print('[getComTratamento]inicio')
    try:
        return objeto[nomeCampo]
    except Exception:
        return '-'

def getMaior(objeto):
    print('[getMaior]inicio')
    try:
        if (len(objeto) > 0):
            return True
        else:
            return False
    except Exception:
        print('[getMaior]Ocorreu um incidente com o objeto' + retrieve_name(objeto))
        return False

def parseBoolean(objBol):
    print('[parseBoolean]inicio')
    if objBol:
        return "TRUE"
    else:
        return "FALSE"

def SearchMetadataObj():
    print('[SearchMetadataObj]inicio: ', datetime.datetime, ' timezone: ', datetime.timezone)
    username = ''
    password = ''
    connin_response = get_session_id(username, password)
    session_id = connin_response[0]
    print("[SearchMetadataObj]sessionId:" + session_id)
    server_url = connin_response[1]
    print("server_url:" + server_url)
    myFlows = getWorkflows(session_id, server_url)
    print('[SearchMetadataObj]capturou objetos:', datetime.datetime, ' timezone: ', datetime.timezone)

    caminhoAbsoluto = 'C:/Temp/' #'/projetos/bu/coe/monitor/'
    arquivoOutput = caminhoAbsoluto + 'IICS_All_Objs.csv'
    myFile, spamwriter = criarArquivo(caminhoAbsoluto, arquivoOutput)
    spamwriter.writerow(
        ['workflowId','orgId', 'workflowName','workflowDescription', 'createTime','updateTime',
        'createdBy','updatedBy','errorTaskEmails','successTaskEmails','warningTaskEmails',
        'maxLogs','workflowTaskName','stopOnError','stopOnWarning','taskId','taskType',
        'mappingId', 'verbose', 'lastRunTime', 'parameterFileName', 'srcObjectLabel', 'sourceConnectionId',
        'customQuery', 'extendedObject', 'srcName', 'targetConnectionId', 'targetObject', 'targetObjectLabel'])

    workflow = ''
    workflowId = ''
    orgId = ''
    workflowName = ''
    workflowDescription = ''
    createTime = ''
    updateTime = ''
    createdBy = ''
    updatedBy = ''
    errorTaskEmails = ''
    successTaskEmails = ''
    warningTaskEmails = ''
    maxLogs = 0
    tasks = ''
    workflowTask = ''
    workflowTaskName = ''
    stopOnError = False
    stopOnWarning = False
    taskId = ''
    taskType = ''

    #mctfields
    mappingId = ''
    verbose = ''
    lastRunTime = ''
    parameterFileName = ''
    srcObjectLabel = ''
    sourceConnectionId = ''
    customQuery = ''
    extendedObject = ''
    srcName = ''
    targetConnectionId = ''
    targetObject = ''
    targetObjectLabel = ''

    for flow in myFlows:
        print(flow)
        workflowId = getComTratamento(flow, 'id')
        orgId = getComTratamento(flow, 'orgId')
        workflowName = getComTratamento(flow, 'name')
        workflowDescription = getComTratamento(flow, 'workflowDescription')
        createTime = getComTratamento(flow, 'createTime')
        updateTime = getComTratamento(flow, 'updateTime')
        createdBy = getComTratamento(flow, 'createdBy')
        updatedBy = getComTratamento(flow, 'updatedBy')

        errorTaskEmailList = getComTratamento(flow, 'errorTaskEmail')
        errorTaskEmails = getComTratamento(errorTaskEmailList, 'emails')

        successTaskEmailList = getComTratamento(flow, 'successTaskEmail')
        successTaskEmails = getComTratamento(successTaskEmailList, 'emails')

        warningTaskEmailList = getComTratamento(flow, 'warningTaskEmail')
        warningTaskEmails = getComTratamento(warningTaskEmailList, 'emails')
        
        maxLogs = 10 #getComTratamento(flow, 'maxLogs')

        tasks = getComTratamento(flow, 'tasks')
        print('tasks: INICIO')
        print(tasks)
        print('tasks: FIM')
        for task in tasks:
            workflowTaskName = getComTratamento(task, 'name')
            print('workflowTaskName:')
            print(workflowTaskName)
            stopOnError = getComTratamento(task, 'stopOnError')
            print(stopOnError)            
            stopOnWarning = getComTratamento(task, 'stopOnWarning')
            print(stopOnWarning)            
            taskId = getComTratamento(task, 'taskId')
            print(taskId)            
            taskType = getComTratamento(task, 'type')
            print(taskType)            

            mcts = SearchMetadataMCT(session_id, server_url, taskId)
            print("mcts:")
            print(mcts)
            mct = mcts
            #for mct in mcts:
            print("mct:")
            print(mct)
            idMCT = getComTratamento(mct, 'id')
            print("idMCT: "+idMCT)
            mappingId = getComTratamento(mct, 'mappingId')
            verbose = getComTratamento(mct, 'verbose')
            lastRunTime = getComTratamento(mct, 'lastRunTime')
            parameterFileName = getComTratamento(mct, 'parameterFileName')
            params = getComTratamento(mct, 'parameters')
            for param in params:
                srcObjectLabel = getComTratamento(param, 'label')
                paramType = getComTratamento(param, 'type')
                print(paramType)
                sourceConnectionId = getComTratamento(param, 'sourceConnectionId')
                print(sourceConnectionId)
                if paramType == 'EXTENDED_SOURCE' or paramType == 'SOURCE':
                    sourceConnectionId = getComTratamento(param, 'sourceConnectionId')
                    print(sourceConnectionId)
                    if paramType == 'EXTENDED_SOURCE' or paramType == 'SOURCE':
                        customQuery = getComTratamento(param, 'customQuery')
                    uiProperties = getComTratamento(param, 'uiProperties')
                    for entry in uiProperties:
                        entryValue = getComTratamento(entry, 'value')
                        if entryValue == 'Flat File' : ## flat file
                            break
                        elif entryValue == 'Flat File Or Relational Database': #ODBC
                            break
                    extendedObject = getComTratamento(param, 'extendedObject')
                    #srcName = getComTratamento(extendedObject[0], 'name')
                elif paramType == 'TARGET':
                    targetConnectionId = getComTratamento(param, 'targetConnectionId')
                    targetObject = getComTratamento(param, 'targetObject')
                    targetObjectLabel = getComTratamento(param, 'targetObjectLabel')
                elif paramType == 'STRING':
                    print('Type STRING')
                print(mappingId)
                print(verbose)
                print(lastRunTime)
                print(parameterFileName)
                print(srcObjectLabel)
                print(sourceConnectionId)
                print(customQuery)
                print(extendedObject)
                print(srcName)
                print(targetConnectionId)
                print(targetObject)
                print(targetObjectLabel)
                spamwriter.writerow([workflowId, orgId, workflowName, workflowDescription, createTime, updateTime,
                createdBy, updatedBy, errorTaskEmails, successTaskEmails, warningTaskEmails,
                maxLogs, workflowTaskName, stopOnError, stopOnWarning, taskId, taskType, 
                mappingId, verbose, lastRunTime, parameterFileName, srcObjectLabel, 
                sourceConnectionId, customQuery, extendedObject, srcName, targetConnectionId, 
                targetObject, targetObjectLabel])
        
    print('[SearchMetadataObj]finalizando :', datetime.datetime, ' timezone: ', datetime.timezone)
    fechaArquivo(myFile)
    print('[SearchMetadataObj]fechamento arquivo csv :'+arquivoOutput, datetime.datetime, ' timezone: ', datetime.timezone)

def main():
    print('[main]inicio:', datetime.datetime, ' timezone: ', datetime.timezone)
    print("[main]entrando no SearchMetadataObj")
    SearchMetadataObj()
    print('[main]fim:', datetime.datetime, ' timezone: ', datetime.timezone)

main()
