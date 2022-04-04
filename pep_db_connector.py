import psycopg2
import os.path
import csv
import json
import yaml
import sys
from pprint import pprint



class PepDB:
    """
    A class to connect to pep-db and upload, download, read and process pep projects.
    """

    def __init__(self, host="localhost", port=5432, database="pep-base-sql", user="postgres", password="docker"):
        try:
            self.postgresConnection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password)


        except psycopg2.Error as e:
            print(f"Error occurred while connecting to db {e}")

    def commit_connection(self):
        self.postgresConnection.commit()

    def add_samples_to_db(self, data):
        cursor = self.postgresConnection.cursor()

        sql = """INSERT INTO samples(sample)
                 VALUES (%s) RETURNING id;"""

        data = json.dumps(data)
        cursor.execute(sql, (data,))
        id_of_new_row = cursor.fetchone()[0]

        cursor.close()

        return id_of_new_row

    def add_subsamples_to_db(self, data):
        cursor = self.postgresConnection.cursor()

        sql = """INSERT INTO subsamples(subsample)
                 VALUES (%s) RETURNING id;"""

        data = json.dumps(data)
        cursor.execute(sql, (data,))
        id_of_new_row = cursor.fetchone()[0]

        cursor.close()

        return id_of_new_row

    def add_project_data_to_db(self,  prj_name, prj_file, sample_id=None, subsample_id=None):
        cursor = self.postgresConnection.cursor()

        sql = """INSERT INTO projects(project_name, project_file, sample_id, subsample_id)
                 VALUES (%s, %s, %s, %s) RETURNING id;"""

        data = json.dumps(prj_file)
        cursor.execute(sql, (prj_name, data, sample_id, subsample_id,))
        id_of_new_row = cursor.fetchone()[0]

        cursor.close()
        print(f"project {prj_name} has been added successfully")
        return id_of_new_row

    def read_csv_file(self, file_path):
        rows = []
        # fields = []
        with open(file_path, 'r') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)

            # extracting field names through first row
            fields = next(csvreader)

            for row in csvreader:
                rows.append(row)

        return fields, rows

    def csv_to_json(self, fields, rows):
        dict_meta = {"col_names": fields,
                     "metadata": []}

        for row in rows:
            meta_rows = {}
            for item_nb in range(len(row)):
                meta_rows[fields[item_nb]] = row[item_nb]

            dict_meta["metadata"].append(meta_rows)

        return dict_meta

    def json_to_csv(self, json_file):
        fields = json_file["col_names"]
        rows = []
        for sample in json_file["metadata"]:
            rows.append([])
            for field in fields:
                rows[len(rows) - 1].append(sample[field])

        return fields, rows

    def open_csv_in_json(self, csv_path):
        fields, rows = self.read_csv_file(csv_path)
        data = self.csv_to_json(fields, rows)
        return data

    @staticmethod
    def open_project_file(project_path):
        with open(project_path, "r") as stream:
            try:
                project_data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return project_data

    @staticmethod
    def get_sample_path(project_config, project_config_path, project_key):
        try:
            sample_table_file = project_config[project_key]
            if not os.path.exists(sample_table_file):
                dir_path = os.path.split(project_config_path)[0]
                sample_table_file = os.path.join(dir_path, sample_table_file)
                if not os.path.exists(sample_table_file):
                    return None
            return sample_table_file
        except KeyError:
            return None

    def upload_project(self, name, project_path):
        project_data = self.open_project_file(project_path)

        sample_path = self.get_sample_path(project_data, project_path, "sample_table")
        subsample_path = self.get_sample_path(project_data, project_path, "subsample_table")

        if sample_path is not None:
            sample_data = self.open_csv_in_json(sample_path)
            sample_id = self.add_samples_to_db(sample_data)
        else:
            sample_id = None

        if subsample_path is not None:
            sub_data = self.open_csv_in_json(subsample_path)
            sub_id = self.add_subsamples_to_db(sub_data)
        else:
            sub_id = None

        self.add_project_data_to_db(name, project_data, sample_id, sub_id)
        self.postgresConnection.commit()

    def download_project(self, name=None, id=None, downloads_path=None):
        if downloads_path is None:
            print("downloads_path is None!")
            downloads_path = os.getcwd()


        cursor = self.postgresConnection.cursor()
        sql_q = """
                select p.project_name, p.project_file, samp.sample, sub.subsample  from projects as p 
                    left join subsamples as sub on p.subsample_id=sub.id 
                    left join samples as samp on p.sample_id=samp.id
                """
        if name is not None:
            sql_q = sql_q + """ where p.project_name=%s;"""
            cursor.execute(sql_q, (name,))

        elif id is not None:
            sql_q = sql_q + """ where p.id=%s; """
            cursor.execute(sql_q, (id,))

        else:
            print("You haven't provided neither name nor id! Execution is unsuccessful")
            print("Files haven't been downloaded")
            return None

        found_prj = cursor.fetchone()

        proj_name = found_prj[0]
        project_data = found_prj[1]
        downloads_path = os.path.join(downloads_path, proj_name)
        self.check_path(downloads_path)

        # converting json to csv
        if found_prj[2] is not None:
            sample_data = self.json_to_csv(found_prj[2])
            print(sample_data)
            sample_file_name = os.path.split(found_prj[1]['sample_table'])[1]
            self.save_csv_file(sample_data, os.path.join(downloads_path, sample_file_name))

            project_data["sample_table"] = sample_file_name

        if found_prj[3] is not None:
            subsample_data = self.json_to_csv(found_prj[3])
            subsample_file_name = os.path.split(found_prj[1]['subsample_table'])[1]
            print(subsample_data)
            self.save_csv_file(subsample_data, os.path.join(downloads_path, subsample_file_name))

            project_data["subsample_table"] = subsample_file_name

        self.save_yaml_file(project_data, os.path.join(downloads_path, "project_config.yaml"))

        # print(downloads_path)
        # pprint(found_prj)
        # print(found_prj)

    def print_all_projects(self):
        cursor = self.postgresConnection.cursor()
        sql_q = """
                select project_name, id, sample_id, subsample_id from projects;
                """
        cursor.execute(sql_q)

        id_of_new_row = cursor.fetchall()
        for d in id_of_new_row:
            print(d)

    def retrieve_single_project(self, name=None, proj_id=None):
        cursor = self.postgresConnection.cursor()
        try:
            if name is not None:
                sql = """select * from samples where id = (select sample_id from projects 
                        where project_name=%s LIMIT 1) ;"""
                cursor.execute(sql, (name,))
            elif id is not None:
                sql = """select * from samples where id = (select sample_id from projects 
                        where id=%s LIMIT 1) ;"""
                cursor.execute(sql, (proj_id,))
            else:
                return None
            id_of_new_row = cursor.fetchone()[1]
        except psycopg2.Error as e:
            print(f"{e}")
            cursor.close()
            return None
        print(id_of_new_row)
        aa = self.json_to_csv(id_of_new_row)
        print(aa)
        cursor.close()

        return aa

    @staticmethod
    def save_yaml_file(data, f_path):
        with open(f_path, 'w+') as f:
            yaml.dump(data, f, allow_unicode=True)
            print("done")

    @staticmethod
    def save_csv_file(data, f_path):
        fields, rows = data

        with open(f_path, 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)

            write.writerow(fields)
            write.writerows(rows)


    @staticmethod
    def check_path(path):
        is_exist = os.path.exists(path)
        print(is_exist)
        if not is_exist:
            # Create a new directory because it does not exist
            os.makedirs(path)

def main():
    project = PepDB()
    # a = project.retrieve_single_project(id=2)
    # project.upload_project(name="BiocProject",
    #                        project_path="/home/bnt4me/Virginia/pephub_db/sample_pep/BiocProject/project_config.yaml")
    project.download_project(id=6)
    # project.print_all_projects()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)



