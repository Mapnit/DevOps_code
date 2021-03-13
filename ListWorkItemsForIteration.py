import argparse
import copy
import os
from datetime import datetime

import pytz
from azure.devops.connection import Connection
from azure.devops.v5_1.work import models as workModels
from azure.devops.v5_1.work_item_tracking import \
    models as workItemTrackingModels
from msrest.authentication import BasicAuthentication
from openpyxl import Workbook

# DevOps personal access token (recycle every 30 days) 
personal_access_token = 'PAT-pat'
# DevOps Organization URL
organization_url = 'https://dev.azure.com/YOUR-ORG'

usa_cst = pytz.timezone('US/Central')
today_timezone = usa_cst.localize(datetime.now())

# Create a connection to the org
credentials = BasicAuthentication('', personal_access_token)
connection = Connection(base_url=organization_url, creds=credentials)

class experiment: 
    def get_projects():
        # Get a client (the "core" client provides access to projects, teams, etc)
        core_client = connection.clients.get_core_client()

        # Get the first page of projects
        get_projects_response = core_client.get_projects()

        index = 0
        if get_projects_response is not None:
            for project in get_projects_response.value:
                print("Project [" + str(index) + "]: " + project.name)
                index += 1
            if get_projects_response.continuation_token is not None and get_projects_response.continuation_token != "":
                # Get the next page of projects
                get_projects_response = core_client.get_projects(continuation_token=get_projects_response.continuation_token)
            else:
                # All projects have been retrieved
                get_projects_response = None

    def get_teams():
        project_name = 'CNP.GIS'
        # Get a client (the "core" client provides access to projects, teams, etc)
        core_client = connection.clients.get_core_client()
        # get the teams
        index = 0
        get_teams_response = core_client.get_teams(project_name) 
        if get_teams_response is not None: 
            for team in get_teams_response: 
                print("Team [" + str(index) + "]: " + team.name)
                index += 1
            # All teams have been retrieved
            get_teams_response = None

    def get_boards(): 
        # get the boards 
        index = 0 
        get_boards_response = work_client.get_boards(team_context)

        if get_boards_response is not None: 
            for board in get_boards_response: 
                print("Board [" + str(index) + "]: " + board.name)
                index += 1
            # All boards have been retrieved
            get_teams_response = None

work_item_type_of_interest = ["Product Backlog Item", "Task"]

wiql_template = "\
    Select [System.Id] From WorkItems \
    Where [System.AreaPath] = 'CNP.GIS' \
        and ( [System.WorkItemType] = 'Product Backlog Item' \
            or [System.WorkItemType] = 'Task' ) \
        and [System.IterationPath] = '#IterationPath#' \
    Order by [Microsoft.VSTS.Common.Priority] asc, [System.CreatedDate] desc\
"

field_names = ['System.Id', 'System.WorkItemType', 'System.Parent', 
    'System.Title', 'System.Description', 'System.Tags', 
    'Microsoft.VSTS.Common.ValueArea', 'Microsoft.VSTS.Common.BusinessValue', 
    'System.AssignedTo', 'System.State', 'System.CreatedDate', 'System.ChangedDate',
    'System.AreaPath', 'System.IterationPath']


def get_current_iteration(team_context): 

    work_client = connection.clients.get_work_client()

    current_iteration = None

    # get the iteration paths
    index = 0 
    get_team_iterations_response = work_client.get_team_iterations(team_context, 'current')
    if get_team_iterations_response is not None: 
        for team_iteration in get_team_iterations_response: 
            start_date = team_iteration.attributes.start_date
            if start_date.year < 2021 or start_date > today_timezone: 
                # skip the iterations of last year and the future
                None
            else: 
                current_iteration = team_iteration.path
                print("Iteration [{0}]: {1}, {2} ({3} -> {4})".format(
                    index, team_iteration.name, team_iteration.path, 
                    team_iteration.attributes.start_date.strftime("%Y-%m-%d"),
                    team_iteration.attributes.finish_date.strftime("%Y-%m-%d")
                    ))
                index += 1
        # All team iterations have been retrieved
        get_team_iterations_response = None

    return current_iteration


def retrieve_work_items(team_context, iteration_path):

    # query the backlogs 
    work_tracking_client = connection.clients.get_work_item_tracking_client()

    wiql = workItemTrackingModels.Wiql(query= wiql_template.replace("#IterationPath#", iteration_path))

    query_by_wiql_response = work_tracking_client.query_by_wiql(wiql, team_context)

    index = 0
    idList = []
    parentIdList = []
    work_item_list = []
    if query_by_wiql_response is not None:
        # collect all work item Ids 
        for work_item_id in query_by_wiql_response.work_items: 
            # print("work item [{0}]: {1}".format(index, work_item_id.id))
            index += 1
            idList.append(work_item_id.id)

        # output all work items, including parents not created in this iteration
        i, j = 0, min(200, len(idList))
        while i < len(idList):    
            # get work items in a batch 
            get_work_items_response = work_tracking_client.get_work_items(
                idList[i:j], fields = field_names)
            if get_work_items_response is not None: 
                for work_item in get_work_items_response: 
                    # output to the screen
                    print("{0}, {1}: {2}".format(
                        work_item.fields["System.WorkItemType"], 
                        work_item.fields["System.Title"], 
                        work_item.fields["System.State"]))
                    # save to the return variable
                    work_item_list.append(work_item)

                    if "System.Parent" in work_item.fields.keys():
                        parent_id = work_item.fields["System.Parent"]
                        if parent_id is not None and parent_id not in idList and parent_id not in parentIdList: 
                            # found a parent not in this iteration. now go get it
                            get_work_item_response = work_tracking_client.get_work_item(
                                parent_id, fields = field_names)
                            if get_work_item_response is not None: 
                                parent_work_item = get_work_item_response
                                if parent_work_item.fields["System.WorkItemType"] in work_item_type_of_interest:
                                    # output to the screen
                                    print("{0}, {1}: {2}".format(
                                        parent_work_item.fields["System.WorkItemType"], 
                                        parent_work_item.fields["System.Title"], 
                                        parent_work_item.fields["System.State"])) 
                                    # save to the return variable
                                    work_item_list.append(parent_work_item)
                                    parentIdList.append(parent_id)
                                    index += 1

            i, j = j, min(j + 200, len(idList))

        # All query results have been retrieved
        print("total {0} work items retrieved".format(index))
        idList = None
        query_by_wiql_response = None

        # return all work items
        return work_item_list


def write_to_excel(work_item_list, file_path):

    # prepare the excel file 
    wb = Workbook()
    ws = wb.active
    ws.title = "current_iteration"

    excel_field_names = copy.deepcopy(field_names)
    excel_field_names.append('Excel.Operation') 
    excel_field_excel_operation_index = len(excel_field_names)
    excel_field_names.append('Excel.Region')
    excel_field_excel_region_index = len(excel_field_names)
    excel_field_names.append('Excel.Planned')
    excel_field_excel_planned_index = len(excel_field_names)

    # write the headers to the workbook
    for f in range(len(excel_field_names)): 
        simple_name = excel_field_names[f].split('.')[-1]
        ws.cell(row=1, column=f+1, value=simple_name)

    # write data to the workbook 
    for r in range(len(work_item_list)): 
        work_item = work_item_list[r]
        for f in range(len(excel_field_names)):
            field_name = excel_field_names[f] 
            if field_name in work_item.fields.keys():
                fc = excel_field_names.index(field_name)
                field_value = work_item.fields[field_name]
                if field_name == 'System.Tags': 
                    field_value = field_value.upper()
                    # parse the tags for operation
                    if field_value.find('ELECTRIC') > -1: 
                        ws.cell(row=r+2, column=excel_field_excel_operation_index, value='ELECTRIC')
                    if field_value.find('GAS') > -1: 
                        ws.cell(row=r+2, column=excel_field_excel_operation_index, value='GAS')
                    # parse the tags for region
                    if field_value.find('INOH') > -1: 
                        ws.cell(row=r+2, column=excel_field_excel_region_index, value='INOH')
                    # parse the tags for Planned or Unplanned 
                    if field_value.find('UNPLANNED') > -1: 
                        ws.cell(row=r+2, column=excel_field_excel_planned_index, value='UNPLANNED')
                if field_name == 'System.AssignedTo': 
                    # simplify the AssignedTo object 
                    field_value = work_item.fields["System.AssignedTo"]['displayName']
                ws.cell(row=r+2, column=fc+1, value=field_value)

    # save to a given file
    wb.save(file_path)


if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description='Retrieve work items from Azure DevOps for a given or current iteration.')
    parser.add_argument('-p', '--project', metavar='<Project Name>', default='CNP.GIS',
                        help='ex. CNP.GIS')
    parser.add_argument('-t', '--team', metavar='<Team Name>', default='CNP.GIS Team',
                        help='ex. CNP.GIS Team')
    parser.add_argument('-i', '--iteration', metavar="<Iteration Path>", 
                        help='ex. CNP.GIS\\Sprint 21.03-A')

    args = parser.parse_args()

    # set the team context 
    team_context = workModels.TeamContext(project=args.project, team=args.team)

    if args.iteration is None: 
        print("****** Getting the current iteration ...")
        iteration_path = get_current_iteration(team_context)
    else: 
        iteration_path = args.iteration
        
    print("****** Retrieving work items for {0} ....".format(iteration_path))
    work_item_list = retrieve_work_items(team_context, iteration_path)

    local_folder = os.getcwd()
    file_name = iteration_path.replace('\\', '_') + ".xlsx"
    file_path = os.path.join(os.path.join(local_folder, r"iterations"), file_name)
    if os.path.exists(file_path):
        raise Exception("File ({0}) already exists.".format(file_path))

    print("****** Storing work items to an Excel file {0} ....".format(file_path)) 
    write_to_excel(work_item_list, file_path)

    print("****** Completed")
