import json
from mstr_session import MstrSession


def build_select_choices(choices=None):
    if not choices:
        return {"choices": []}
    if isinstance(choices, str):
        return {"choices": [{"label": "{}".format(choices)}]}
    if isinstance(choices, list):
        return {"choices": choices}
    if isinstance(choices, dict):
        returned_choices = []
        for choice_key in choices:
            returned_choices.append({
                "label": choice_key,
                "value": choices.get(choice_key)
            })


def do(payload, config, plugin_config, inputs):
    parameter_name = payload.get('parameterName')
    base_url = plugin_config.get("base_url", None)
    project_name = config["microstrategy_project"].get("project_name", None)
    project_id = ""  # the project id, obtained through a later request
    dataset_name = str(config.get("dataset_name", None)).replace(" (created by Dataiku DSS)", "") + " (created by Dataiku DSS)"
    dataset_id = ""  # the dataset id, obtained at creation or on update
    table_name = "dss_data"
    username = config["microstrategy_api"].get("username", None)
    password = config["microstrategy_api"].get("password", '')

    if parameter_name == "selected_project_id":
        session = MstrSession(base_url, username, password)
        projects = session.get_projects()
        choices = []
        for project in projects:
            choices.append({
                "label": project.get("name"),
                "value": project.get("id")
            })
        return build_select_choices(choices)
    elif parameter_name == "selected_folder_id":
        selected_project_id = config.get("selected_project_id")
        if not selected_project_id:
            return build_select_choices("Nothing for now")
        saved_structure = json.loads(config.get("selected_folder_id", "{}"))
        selected_folder_name = saved_structure.get("names", [])
        selected_folder_id = saved_structure.get("ids", [])
        session = MstrSession(base_url, username, password)
        if not selected_folder_id:
            folders = session.get_shared_folders(selected_project_id)
            choices
        else:
            folders = session.get_folder(selected_project_id, selected_folder_id[-1])
            choices = [{"label": "/".join(selected_folder_name), "value": config.get("selected_folder_id", "[]")}]
        for folder in folders:
            if folder.get("type") == 8:
                path_ids = selected_folder_id + ["{}".format(folder.get("id"))]
                path_names = selected_folder_name + ["{}".format(folder.get("name"))]
                full_path = selected_folder_name + [folder.get("name")]
                choices.append({
                    "label": "/".join(full_path),
                    "value": "{}".format(json.dumps({"names": path_names, "ids": path_ids}))
                })
        if len(selected_folder_id) > 0:
            choices.append({
                "label": "🔙 {}".format("/".join(selected_folder_name[:-1])),
                "value": "{}".format(json.dumps({"names": selected_folder_name[:-1], "ids": selected_folder_id[:-1]}))
            })
        return build_select_choices(choices)
