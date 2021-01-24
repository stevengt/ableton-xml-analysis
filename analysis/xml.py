
import os

import pandas as pd
from pandasql import sqldf


class DocumentInfo:

    def __init__(self, etree_root):
        elements_df, attributes_df = self._get_elements_and_attributes_dataframes(etree_root)
        self._dataframes = {
            "ELEMENT": elements_df,
            "ATTRIBUTE": attributes_df
        }
        # self.root_element = self._get_element(0)

    def query(self, sql):
        """
        Returns a dataframe of results returned from executing the provided
        SQL query against tables with the following schema.

        CREATE TABLE "ELEMENT" (
            "ELEMENT_ID" INTEGER,
            "PARENT_ID" INTEGER,
            "TAG" TEXT,
            "CONTENT" TEXT,
            PRIMARY KEY("ELEMENT_ID"),
            FOREIGN KEY("PARENT_ID") REFERENCES "ELEMENT"("ELEMENT_ID")
        );

        CREATE TABLE "ATTRIBUTE" (
            "ATTRIBUTE_ID" INTEGER,
            "ELEMENT_ID" INTEGER,
            "NAME" TEXT,
            "VALUE" TEXT,
            PRIMARY KEY("ATTRIBUTE_ID"),
            FOREIGN KEY("ELEMENT_ID") REFERENCES "ELEMENT"("ELEMENT_ID")
        );
        """
        return sqldf(sql, self._dataframes)

    def get_all_element_tag_names(self, parent_tag_name=None):
        """
        Returns a set of all unique element tag names within the XML document.
        If parent_tag_name is specified, then only the element tags that appear
        with parents of that type are returned.
        """
        sql_query = "SELECT DISTINCT TAG FROM ELEMENT"
        if parent_tag_name is not None:
            sql_query = \
                f"""
                SELECT DISTINCT E1.TAG
                FROM ELEMENT E1
                LEFT JOIN ELEMENT E2
                    ON E1.PARENT_ID = E2.ELEMENT_ID
                WHERE E2.TAG = '{parent_tag_name}'
                """
        tag_names = self.query(sql_query)["TAG"]
        return set(tag_names)

    def get_all_attribute_names(self, element_tag_name=None):
        """
        Returns a set of all unique attribute names within the XML document.
        If element_tag_name is specified, then only the attributes that appear
        in elements of that type are returned.
        """
        sql_query = "SELECT DISTINCT NAME FROM ATTRIBUTE"
        if element_tag_name is not None:
            sql_query = \
                f"""
                SELECT DISTINCT A.NAME
                FROM ATTRIBUTE A
                LEFT JOIN ELEMENT E
                    ON A.ELEMENT_ID = E.ELEMENT_ID
                WHERE E.TAG = '{element_tag_name}'
                """
        attribute_names = self.query(sql_query)["NAME"]
        return set(attribute_names)

    def get_elements_info_grouped_by_attribute(self, attribute, include_single_instances=False,
                                               show_only_unique_tag_names=False):
        """
        Returns a dictionary that maps unique values of the specified attribute
        to a list of 'TAG' and 'ELEMENT_ID' values for elements containing an
        attribute with that value.

        By default, any attribute values that correspond to a single element
        instance are not returned. To include these, set the include_single_instances
        parameter to True.

        If show_only_unique_tag_names is set to True, then the lists corresponding
        to each unique value of the specified attribute will contain only the unique
        tag_names among their element instances, instead of the 'TAG' and 'ELEMENT_ID'
        of each instance.
        """

        attribute_instances = self.query(f"""
                                          SELECT A.VALUE, E.TAG, E.ELEMENT_ID
                                          FROM ATTRIBUTE A
                                          LEFT JOIN ELEMENT E
                                              ON A.ELEMENT_ID = E.ELEMENT_ID
                                          WHERE A.NAME = '{attribute}'
                                          """)

        attribute_values = set(attribute_instances["VALUE"])

        grouped_elements_info = {}

        for val in attribute_values:

            attribute_instances_with_val = attribute_instances[attribute_instances["VALUE"] == val]
            if len(attribute_instances_with_val) == 1 and not include_single_instances:
                continue

            elements_info_list = []
            for _, attribute_instance in attribute_instances_with_val.iterrows():
                elements_info_list.append({
                    "TAG": attribute_instance["TAG"],
                    "ELEMENT_ID": attribute_instance["ELEMENT_ID"]
                })

            if show_only_unique_tag_names:
                elements_info_list = list(set([element_info["TAG"] for element_info in elements_info_list]))

            grouped_elements_info[val] = elements_info_list

        return grouped_elements_info

    def save_as_nested_files(self, root_directory_path="root"):
        """
        Saves the XML document in a specially formatted hierarchy of files
        representing the contents of the XML.

        # Each directory will be named with a number N, such that the
        # directory represents the N'th child element of its parent.

        Each directory will contain these files:
            - "tag": Contains the name of the element's tag
            - "attributes": Contains all of the elements attributes, as
               they appear inline within the XML.

        Each directory may or may not contain:
            - Nested directories representing child XML elements
            - A file named "contents", containing the contents of the XML element.
        """

        # Get a list of lists that each represent a full directory path.
        directory_paths = self._get_directory_paths()
        for directory_path in directory_paths:
            os.makedirs(
                os.path.join(root_directory_path, *directory_path),
                exist_ok=True
            )
            cur_directory = root_directory_path
            for directory in directory_path:
                cur_element_id = directory
                cur_directory = os.path.join(cur_directory, cur_element_id)
                tag, content, attributes = self.get_tag_attributes_and_contents(cur_element_id)
                tag_file_path = os.path.join(cur_directory, "tag")
                contents_file_path = os.path.join(cur_directory, "contents")
                attributes_file_path = os.path.join(cur_directory, "attributes")

                with open(tag_file_path, "w") as tag_file:
                    tag_file.write(tag)
                if content is not None:
                    with open(contents_file_path) as content_file:
                        content_file.write(content)
                with open(attributes_file_path, "w") as attribute_file:
                    attribute_file.write(attributes)

    def get_tag_attributes_and_contents(self, element_id):
        """
        Return three strings containing an element's tag name,
        contents, and attributes as they appear inline in XML.
        """
        query = f"""
                SELECT
                    TAG, CONTENT, NAME, VALUE
                FROM
                         ELEMENT E
                    JOIN ATTRIBUTE A
                         ON E.ELEMENT_ID=A.ELEMENT_ID
                WHERE
                    E.ELEMENT_ID='{element_id}'
                """
        results = self.query(query)
        tag = results["TAG"].iloc[0]
        content = results["CONTENT"].iloc[0]
        attributes = ""
        for i, row in results.iterrows():
            attributes += " "
            attributes += f'''{row["NAME"]}="{row["VALUE"]}"'''
        return tag, content, attributes

    def _get_directory_paths(self):
        paths = []
        list_of_element_ids_with_no_children = self.get_list_of_element_ids_with_no_children()
        for element_id in list_of_element_ids_with_no_children:
            cur_path = [element_id]
            cur_element_id = element_id
            while self.has_parent_element(cur_element_id):
                parent_element_id = self.get_parent_element_id(cur_element_id)
                cur_path.append(parent_element_id)
                cur_element_id = parent_element_id
            cur_path.reverse()
            paths.append(cur_path)
        return paths

    def get_list_of_element_ids_with_no_children(self):
        list_of_element_ids_with_no_children = []
        min_element_id, max_element_id = self.get_min_and_max_element_id()
        for element_id in range(min_element_id, max_element_id + 1):
            if not self.has_child_element(element_id):
                list_of_element_ids_with_no_children.append(element_id)
        return list_of_element_ids_with_no_children

    def get_list_of_child_element_ids(self, element_id):
        query = f"""
                SELECT ELEMENT_ID
                FROM ELEMENT
                WHERE PARENT_ID='{element_id}'
                """
        return list(self.query(query)["ELEMENT_ID"])

    def get_parent_element_id(self, element_id):
        query = f"""
                SELECT PARENT_ID
                FROM ELEMENT
                WHERE ELEMENT_ID='{element_id}'
                """
        return list(self.query(query)["PARENT_ID"])[0]

    def get_min_and_max_element_id(self):
        query = """
                SELECT
                    MIN(ELEMENT_ID) AS MIN,
                    MAX(ELEMENT_ID) AS MAX
                FROM ELEMENT
                """
        results = self.query(query)
        min, max = results["MIN"].iloc[0], results["MAX"].iloc[0]
        return min, max

    def has_child_element(self, element_id):
        list_of_child_element_ids = self.get_list_of_child_element_ids(element_id)
        return len(list_of_child_element_ids) > 0

    def has_parent_element(self, element_id):
        parent_element_id = self.get_parent_element_id(element_id)
        return parent_element_id is not None

    def _get_elements_and_attributes_dataframes(self, etree_root):
        elements_df = self._get_elements_df(etree_root)
        attributes_df = self._get_attributes_df(elements_df)
        elements_df.drop("ETREE_NODE", axis=1, inplace=True)
        return elements_df, attributes_df

    def _get_elements_df(self, etree_root):

        df = pd.DataFrame(columns=["ELEMENT_ID", "PARENT_ID", "ETREE_NODE", "TAG", "CONTENT"])

        for node in etree_root.iter():
            df_row_dict = {
                "PARENT_ID": None,
                "ETREE_NODE": node,
                "TAG": node.tag,
                "CONTENT": node.text.strip() if node.text is not None and not node.text.isspace() else None
            }
            df = df.append(df_row_dict, ignore_index=True)

        df["PARENT_ID"] = df["ETREE_NODE"].apply(lambda node: self._get_parent_index_from_etree_node(df, node)) \
                                          .astype("Int64")

        df["ELEMENT_ID"] = df.index
        return df

    def _get_attributes_df(self, elements_df):

        df = pd.DataFrame(columns=["ATTRIBUTE_ID", "ELEMENT_ID", "NAME", "VALUE"])

        for element_index, element_row in elements_df.iterrows():
            for name, value in element_row["ETREE_NODE"].attrib.items():
                df_row_dict = {
                    "ELEMENT_ID": element_index,
                    "NAME": name,
                    "VALUE": value
                }
                df = df.append(df_row_dict, ignore_index=True)

        df["ATTRIBUTE_ID"] = df.index
        return df

    def _get_parent_index_from_etree_node(self, df, node):
        is_parent_node = df["ETREE_NODE"] == node.getparent()
        parent_indeces = df.index[is_parent_node].tolist()
        num_parents = len(parent_indeces)
        if num_parents == 0:
            return None
        elif num_parents == 1:
            return int(parent_indeces[0])
        else:
            raise Exception(f"Found more than one parent for node: {etree.tostring(node)}")

    # def _get_element(self, id):
    #     if id is None:
    #         return None
    #     element_row = self._dataframes["ELEMENT"].iloc[id]
    #     name = element_row["TAG"]
    #     content = element_row["CONTENT"]
    #     attribute_rows = self._dataframes["ATTRIBUTE"][self._dataframes["ATTRIBUTE"]["ELEMENT_ID"] == id]
    #     attributes = {row["NAME"]: row["VALUE"] for i, row in attribute_rows.iterrows()}
    #     child_indeces = self._dataframes["ELEMENT"][self._dataframes["ELEMENT"]["PARENT_ID"] == id].index.tolist()
    #     children = [self._get_element(child_id) for child_id in child_indeces]
    #     element = ElementInfo(id, name, attributes, children)
    #     for child in element.children():
    #         child.set_parent(element)
    #     return element


# class ElementInfo:

#     def __init__(self, id, name, attributes, children):
#         self._id = id
#         self._name = name
#         self._attributes = attributes
#         self._parent = None
#         self._children = children

#     def id(self):
#         return self._id

#     def name(self):
#         return self._name

#     def attributes(self):
#         return self._attributes

#     def parent(self):
#         return self._parent

#     def set_parent(self, parent):
#         self._parent = parent

#     def children(self):
#         return self._children

#     def has_children(self):
#         children = self.children()
#         return children is not None and len(children) > 0

#     def get_tag_string(self):
#         s = f"<{self.name()}"
#         if len(self.attributes().keys()) > 0:
#             for attribute, value in self.attributes().items():
#                 s += f' {attribute}="{VALUE}"'
#             s += " "
#         if self.has_children():
#             s += ">"
#         else:
#             s += "/>"
#         return s

#     def __str__(self):
#         s = self.get_tag_string()
#         if self.has_children():
#             for child in self.children():
#                 s += f"\n\t{child.get_tag_string()}"
#             s += f"\n</{self.name()}>"
#         return s
