from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from tobq import grabar
import os
import time
from config import readjson
start = time.time()

def login(sharepoint_base_url):
  from connections import accesos
  #target url taken from sharepoint and credentials
  url='https://99minutos.sharepoint.com/'
  sharepoint_user = accesos['user']
  sharepoint_password = accesos['password']
  #https://99minutos.sharepoint.com/sites/Rentabilidad99minutos/_api/Web/GetFolderByServerRelativePath(decodedurl='/sites/Rentabilidad99minutos/')/Folders
  auth = AuthenticationContext(sharepoint_base_url)
  auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
  ctx = ClientContext(sharepoint_base_url, auth)
  web = ctx.web
  ctx.load(web)
  ctx.execute_query()
  print('Connected to SharePoint: ', web.properties['Title'])
  return ctx


# Constructing Function for getting file details in SharePoint Folder

def folder_details(ctx, folder_in_sharepoint):
  folder = ctx.web.get_folder_by_server_relative_url(folder_in_sharepoint)
  fold_names = []
  sub_folders = folder.files
  ctx.load(sub_folders)
  ctx.execute_query()
  for s_folder in sub_folders:
    fold_names.append(s_folder.properties["Name"])


  return fold_names

for path in os.listdir('./configs'):
    data=readjson('./configs/'+path)
    sharepoint_base_url=data['sharepoint_base_url']
    folder_in_sharepoint=data['folder_in_sharepoint']
    schemabq=data['schemaBigquery']
    tableBigquery = data['TableBigQuery']
    columns=data['columns']
    ctx=login(sharepoint_base_url)
    file_list = folder_details(ctx, folder_in_sharepoint)
    for i in file_list:
      file_url = folder_in_sharepoint + "/" + i
      download_path = "./files/"+i
      with open(download_path, "wb") as local_file:
        file = ctx.web.get_file_by_server_relative_path(file_url).download(local_file).execute_query()
        if 'xlsx' in i:
          grabar(schemabq,tableBigquery,download_path,columns)


end = time.time()
print(end - start)