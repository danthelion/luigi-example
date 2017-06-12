import os
import json

from collections import Counter
from typing import List, Dict

import luigi


class InputText(luigi.ExternalTask):
    """
    This class represents something that was created elsewhere by an external process,
    so all we want to do is to implement the output method.
    """
    input_file: str = luigi.Parameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, it expects a file to be present in the local file system.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(self.input_file)


class WordCount(luigi.Task):
    input_dir = luigi.Parameter()

    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.InputText`
        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return [InputText('{}/{}'.format(self.input_dir, f)) for f in os.listdir(str(self.input_dir))]

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget('word_count.json')

    def run(self):
        """
        1. Count the occurence of words for each of the InputText output targets created by InputText.
        2. Write the count into the WordCount.output target.
        """

        merged_file_content: List[str] = self.merge_files(file_path_list=self.input())

        word_counter: Dict[str, int] = Counter(merged_file_content)

        with self.output().open('w') as outfile:
            outfile.write(json.dumps(word_counter))

    def merge_files(self, file_path_list: List[luigi.LocalTarget]) -> List[str]:
        """
        Merge contents of multiple files into a list of words (strings.)

        :param file_path_list: List of luigi.LocalTarget objects, luigis inner representation of file target.
        :return:
        :rtype: list
        """
        if not file_path_list:
            return []
        else:
            f: luigi.LocalTarget = file_path_list.pop(0)
            with f.open('r') as infile:
                return infile.read().split() + self.merge_files(file_path_list)


if __name__ == '__main__':
    luigi.run(['--input-dir', 'input_data'],
              main_task_cls=WordCount)
