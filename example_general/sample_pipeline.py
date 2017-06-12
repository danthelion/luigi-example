import luigi


class ExamplePythonTask(luigi.Task):
    def requires(self):
        """
        Which other Tasks need to be complete before
        this Task can start?
        """
        return [OtherTask()]

    def output(self):
        """
        Where will this Task produce output?
        """
        return luigi.LocalTarget('path/to/task/output')

    def run(self):
        """
        How do I run this Task?
        """
        # We can do anything we want in here, from calling python
        # methods to running shell scripts to calling APIs


if __name__ == '__main__':
    luigi.run()

