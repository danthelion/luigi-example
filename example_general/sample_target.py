import luigi


class MyTarget(luigi.Target):
    def exists(self):
        """
        Does this Target exist?
        """
        return True
