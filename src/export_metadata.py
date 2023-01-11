import os
from supervisely.io.json import load_json_file
import supervisely as sly
from supervisely.app.v1.app_service import AppService

my_app = AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ['modal.state.slyProjectId'])
DATASET_ID = os.environ.get('modal.state.slyDatasetId', None)
if DATASET_ID is not None:
    DATASET_ID = int(DATASET_ID)


def get_meta_from_dataset(api, res_dataset, dataset_id, app_logger):
    img_counter = 0
    images = api.image.get_list(dataset_id)
    for image in images:
        if len(image.meta) >= 1:
            res_image_meta_path = os.path.join(res_dataset, image.name + '.json')
            sly.io.json.dump_json_file(image.meta, res_image_meta_path)

        if len(image.meta) == 0:
            img_counter = img_counter + 1
            app_logger.info(f"{image.name} in {os.path.basename(os.path.normpath(res_dataset))} dataset does not contain metadata.")
            continue

    if img_counter >= 1:
        app_logger.warn(
            f"{img_counter}/{len(images)} images in {os.path.basename(os.path.normpath(res_dataset))} dataset does not contain metadata. {img_counter} Images will be skipped.")

    if img_counter == len(images):
        raise Exception("No metadata to download")


@my_app.callback("export_project_images_metadata")
@sly.timeit
def export_project_images_metadata(api: sly.Api, task_id, context, state, app_logger):
    project = api.project.get_info_by_id(PROJECT_ID)
    result_dir_name = "{}_{}".format(project.id, project.name)
    RESULT_DIR = os.path.join(my_app.data_dir, result_dir_name, result_dir_name)
    sly.fs.mkdir(RESULT_DIR)
    ARCHIVE_NAME = result_dir_name + ".tar"
    RESULT_ARCHIVE = os.path.join(my_app.data_dir, ARCHIVE_NAME)
    if DATASET_ID:
        dataset_info = api.dataset.get_info_by_id(DATASET_ID)
        progress = sly.Progress('Get meta from images in {!r} dataset'.format(dataset_info.name), len(api.dataset.get_list(PROJECT_ID)))
        res_dataset = os.path.join(RESULT_DIR, dataset_info.name)
        sly.fs.mkdir(res_dataset)
        get_meta_from_dataset(api, res_dataset, DATASET_ID, app_logger)
    else:
        datasets = api.dataset.get_list(PROJECT_ID)
        for dataset in datasets:
            progress = sly.Progress('Get meta from images in dataset {}'.format(dataset.name), len(datasets),
                                    app_logger)
            res_dataset = os.path.join(RESULT_DIR, dataset.name)
            sly.fs.mkdir(res_dataset)
            get_meta_from_dataset(api, res_dataset, dataset.id, app_logger)

    RESULT_DIR = os.path.join(my_app.data_dir, result_dir_name)
    sly.fs.archive_directory(RESULT_DIR, RESULT_ARCHIVE)
    app_logger.info("Result directory is archived")
    progress.iter_done_report()
    remote_archive_path = os.path.join(
        sly.team_files.RECOMMENDED_EXPORT_PATH, "ApplicationsData/Export-Metadata/{}/{}".format(task_id, ARCHIVE_NAME))

    upload_progress = []
    def _print_progress(monitor, upload_progress):
        if len(upload_progress) == 0:
            upload_progress.append(sly.Progress(message="Upload {!r}".format(ARCHIVE_NAME),
                                                total_cnt=monitor.len,
                                                ext_logger=app_logger,
                                                is_size=True))
        upload_progress[0].set_current_value(monitor.bytes_read)

    file_info = api.file.upload(TEAM_ID, RESULT_ARCHIVE, remote_archive_path, lambda m: _print_progress(m, upload_progress))
    app_logger.info("Uploaded to Team-Files: {!r}".format(file_info.storage_path))
    api.task.set_output_archive(task_id, file_info.id, ARCHIVE_NAME, file_url=file_info.storage_path)

    sly.fs.remove_dir(RESULT_DIR)

    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "PROJECT_ID": PROJECT_ID
    })

    # Run application service
    my_app.run(initial_events=[{"command": "export_project_images_metadata"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)
