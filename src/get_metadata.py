import os
from supervisely_lib.io.json import load_json_file
import supervisely_lib as sly
from supervisely_lib.api.module_api import ApiField


my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ['modal.state.slyProjectId'])
DATASET_ID = os.environ.get('modal.state.slyDatasetId', None)
if DATASET_ID is not None:
    DATASET_ID = int(DATASET_ID)


def get_meta_from_dataset(api, res_dataset, dataset_id):
    images = api.image.get_list(dataset_id)
    for image in images:
        res_image_meta_path = os.path.join(res_dataset, image.name + '.json')
        sly.io.json.dump_json_file(image.meta, res_image_meta_path)


@my_app.callback("add_metadata_from_image")
@sly.timeit
def add_metadata_from_image(api: sly.Api, task_id, context, state, app_logger):
    project = api.project.get_info_by_id(PROJECT_ID)
    result_dir_name = "{}_{}".format(project.id, project.name)

    RESULT_DIR = os.path.join(my_app.data_dir, result_dir_name)
    sly.fs.mkdir(RESULT_DIR)
    ARCHIVE_NAME = result_dir_name + ".tar.gz"
    RESULT_ARCHIVE = os.path.join(my_app.data_dir, ARCHIVE_NAME)
    if DATASET_ID:
        dataset_info = api.dataset.get_info_by_id(DATASET_ID)
        progress = sly.Progress('Get meta from images in dataset'.format(dataset_info.name), app_logger)
        res_dataset = os.path.join(RESULT_DIR, dataset_info.name)
        sly.fs.mkdir(res_dataset)
        get_meta_from_dataset(api, res_dataset, DATASET_ID)
    else:
        datasets = api.dataset.get_list(PROJECT_ID)
        for dataset in datasets:
            progress = sly.Progress('Get meta from images in dataset {}'.format(dataset.name), len(datasets),
                                    app_logger)
            res_dataset = os.path.join(RESULT_DIR, dataset.name)
            sly.fs.mkdir(res_dataset)
            get_meta_from_dataset(api, res_dataset, dataset.id)

    sly.fs.archive_directory(RESULT_DIR, RESULT_ARCHIVE)
    app_logger.info("Result directory is archived")

    progress.iter_done_report()

    remote_archive_path = "/meta_from_images/{}/{}".format(task_id, ARCHIVE_NAME)

    # @TODO: uncomment only for debug
    api.file.remove(TEAM_ID, remote_archive_path)

    upload_progress = []
    def _print_progress(monitor, upload_progress):
        if len(upload_progress) == 0:
            upload_progress.append(sly.Progress(message="Upload {!r}".format(ARCHIVE_NAME),
                                                total_cnt=monitor.len,
                                                ext_logger=app_logger,
                                                is_size=True))
        upload_progress[0].set_current_value(monitor.bytes_read)

    file_info = api.file.upload(TEAM_ID, RESULT_ARCHIVE, remote_archive_path, lambda m: _print_progress(m, upload_progress))
    app_logger.info("Uploaded to Team-Files: {!r}".format(file_info.full_storage_url))
    api.task.set_output_archive(task_id, file_info.id, ARCHIVE_NAME, file_url=file_info.full_storage_url)



    sly.fs.remove_dir(RESULT_DIR)

    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "PROJECT_ID": PROJECT_ID
    })

    # Run application service
    my_app.run(initial_events=[{"command": "add_metadata_from_image"}])


if __name__ == "__main__":
    # @TODO: uncomment only for debug
    sly.fs.clean_dir(my_app.data_dir)
    sly.main_wrapper("main", main)