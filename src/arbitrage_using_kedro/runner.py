from kedro.runner import AbstractRunner, SequentialRunner


class CustomRunner(AbstractRunner):
    def __init__(self, *args, **kwargs):
        self.runner = SequentialRunner(*args, **kwargs)

    def run(self, *args, **kwargs):
        return self.runner.run_only_missing(*args, **kwargs)

    def _run(self, pipeline, catalog):
        return super()._run(pipeline, catalog)

    def create_default_data_set(self, ds_name):
        return super().create_default_data_set(ds_name)
