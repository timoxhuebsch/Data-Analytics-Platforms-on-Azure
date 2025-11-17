from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

api.dataset_download_file(
    'valakhorasani/mobile-device-usage-and-user-behavior-dataset',  # Kaggle dataset slug
    'user_behavior_dataset.csv',
    path='.'
)

api.dataset_download_files('zynicide/wine-reviews', path='.', unzip=True)
