import argparse
import json
import time
import os
import sys
import logging
from flask import Flask


# Creating a logger object
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


class StreamingSource():
    def __init__(self, delay: int, dir_path: str):
        """streaming APIs from json files
        Args:
            delay (int): the delay of streaming API in second between each data point.
            dir_path (str): the directory path of the source files
        """
        self.delay = delay
        self.dir_path = dir_path
        self.files_ls = []
        self.app = Flask(__name__)

    def _get_files(self):
        """ getting all *.json files in the source dir"""
        if os.path.exists(self.dir_path):
            for file in os.listdir(self.dir_path):
                # only take json files
                if file.endswith("json"):
                    path = os.path.join(self.dir_path, file)
                    self.files_ls.append(path)
        else:
            _logger.error(f"Folder does not exist!: {self.dir_path}")
            raise FileExistsError
            exit(1)

    def stream_data(self, file_path: str):
        """create a generator func from given json file

        Args:
            file_path (str): the json file path

        Returns:
            generator : a generator of json object with a self.delay between each data point
        """
        try:
            with open(file_path, "r") as in_f:
                # assuming data fits in memory
                data = json.load(in_f)
                # sort the data by the time key
                data.sort(key=lambda x: x['time'])

            def generate():
                for item in data:
                    yield json.dumps(item) + "\n"
                    time.sleep(self.delay)
            return generate
        except Exception as e:
            _logger.error(
                "Failed to create a data generator for {file_path}, e")
        finally:
            in_f.close()

    def _register_routes(self):
        """register different routes with costume response func to the app using json file name as the end point.
        """
        for path in self.files_ls:
            # getting the name of the file
            end_point = os.path.basename(path).split('.')[0]
            route = f'/stream_data_{end_point}'
            # Generate the API response function
            response_func_stream = self._generate_api_response(
                self.stream_data(path), end_point=end_point)
            # Register the route with the generated response function
            self.app.add_url_rule(route, view_func=response_func_stream)

    def _generate_api_response(self, generate_func, end_point):
        """Generate a custom response of the api"""
        def response_func_stream():
            return self.app.response_class(generate_func(), mimetype='application/json')
        # need to change the name of the func other wise it will be overwritten
        response_func_stream.__name__ = end_point
        return response_func_stream

    def run(self):
        """ The main method """
        self._get_files()
        self._register_routes()
        self.app.run(host='0.0.0.0')


if __name__ == "__main__":
    cmd_input = argparse.ArgumentParser(
        description="Streaming app")
    delay_int = cmd_input.add_argument(
        "delay", type=float, help="The delay in streaming data points")
    args = cmd_input.parse_args()

    dir_path = os.path.join(os.path.dirname(
        os.path.dirname(__file__)), "data_source")
    assert args.delay != "None", "delay int must be integer"
    print(f"-------------- Delay to: {args.delay} sec --------------")
    app = StreamingSource(delay=args.delay, dir_path=dir_path)
    app.run()
