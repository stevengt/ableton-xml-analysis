
import pandas as pd
from pandasql import sqldf


class DocumentInfo:

    def __init__(self, etree_root):
        elements_df, attributes_df = self._get_elements_and_attributes_dataframes(etree_root)
        self._dataframes = {
            "ELEMENTS": elements_df,
            "ATTRIBUTES": attributes_df
        }
        # self.root_element = self._get_element(0)

    def query(self, sql):
        return sqldf(sql, self._dataframes)

    def get_all_element_tag_names(self, parent_tag_name=None):
        """
        Returns a set of all unique element tag names within the XML document.
        If parent_tag_name is specified, then only the element tags that appear
        with parents of that type are returned.
        """
        sql_query = "SELECT DISTINCT TAG_NAME FROM ELEMENTS"
        if parent_tag_name is not None:
            sql_query = f"""
            SELECT DISTINCT E1.TAG_NAME
            FROM ELEMENTS E1
            LEFT JOIN ELEMENTS E2
                ON E1.PARENT_ID = E2.ID
            WHERE E2.TAG_NAME = '{parent_tag_name}'"""
        tag_names = self.query(sql_query)["TAG_NAME"]
        return set(tag_names)

    def get_all_attribute_names(self, element_tag_name=None):
        """
        Returns a set of all unique attribute names within the XML document.
        If element_tag_name is specified, then only the attributes that appear
        in elements of that type are returned.
        """
        attributes = set()
        for index, attribute_row in self._dataframes["ATTRIBUTES"].iterrows():
            name = attribute_row["NAME"]
            if element_tag_name is not None:
                element_index = attribute_row["ELEMENT_ID"]
                element = self._dataframes["ELEMENTS"].iloc[element_index]
                if element["TAG_NAME"] == element_tag_name:
                    attributes.add(name)
            else:
                attributes.add(name)
        return attributes

    def get_elements_info_grouped_by_attribute(self, attribute, include_single_instances=False,
                                               show_only_unique_tag_names=False):
        """
        Returns a dictionary that maps unique values of the specified attribute
        to a list of 'TAG_NAME' and 'ID' values for elements containing an
        attribute with that value.

        By default, any attribute values that correspond to a single element
        instance are not returned. To include these, set the include_single_instances
        parameter to True.

        If show_only_unique_tag_names is set to True, then the lists corresponding
        to each unique value of the specified attribute will contain only the unique
        tag_names among their element instances, instead of the 'TAG_NAME' and 'ID'
        of each instance.
        """
        grouped_elements_info = {}
        attribute_instances = self._dataframes["ATTRIBUTES"][self._dataframes["ATTRIBUTES"]["NAME"] == attribute]
        attribute_values = set(attribute_instances["VALUE"].tolist())

        for val in attribute_values:
            element_indeces = attribute_instances[attribute_instances["VALUE"] == val]["ELEMENT_ID"].tolist()
            if len(element_indeces) == 1 and not include_single_instances:
                continue

            elements_info_list = []
            for element_index in element_indeces:
                elements_info_list.append({
                    "TAG_NAME": self._dataframes["ELEMENTS"].iloc[element_index]["TAG_NAME"],
                    "ID": element_index
                })

            if show_only_unique_tag_names:
                elements_info_list = list(set([element_info["TAG_NAME"] for element_info in elements_info_list]))

            grouped_elements_info[val] = elements_info_list
        return grouped_elements_info

    def _get_elements_and_attributes_dataframes(self, etree_root):
        elements_df = self._get_elements_df(etree_root)
        attributes_df = self._get_attributes_df(elements_df)
        elements_df.drop("ETREE_NODE", axis=1, inplace=True)
        return elements_df, attributes_df

    def _get_elements_df(self, etree_root):

        df = pd.DataFrame(columns=["ETREE_NODE", "TAG_NAME", "CONTENT", "PARENT_ID"])

        for node in etree_root.iter():
            df_row_dict = {
                "ETREE_NODE": node,
                "TAG_NAME": node.tag,
                "CONTENT": node.text.strip() if node.text is not None and not node.text.isspace() else None,
                "PARENT_ID": None
            }
            df = df.append(df_row_dict, ignore_index=True)

        df["PARENT_ID"] = df["ETREE_NODE"].apply(lambda node: self._get_parent_index_from_etree_node(df, node)) \
                                          .astype("Int64")

        df["ID"] = df.index
        return df

    def _get_attributes_df(self, elements_df):

        df = pd.DataFrame(columns=["NAME", "VALUE", "ELEMENT_ID"])

        for element_index, element_row in elements_df.iterrows():
            for name, value in element_row["ETREE_NODE"].attrib.items():
                df_row_dict = {
                    "NAME": name,
                    "VALUE": value,
                    "ELEMENT_ID": element_index
                }
                df = df.append(df_row_dict, ignore_index=True)

        df["ID"] = df.index
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
    #     element_row = self._dataframes["ELEMENTS"].iloc[id]
    #     name = element_row["TAG_NAME"]
    #     content = element_row["CONTENT"]
    #     attribute_rows = self._dataframes["ATTRIBUTES"][self._dataframes["ATTRIBUTES"]["ELEMENT_ID"] == id]
    #     attributes = {row["NAME"]: row["VALUE"] for i, row in attribute_rows.iterrows()}
    #     child_indeces = self._dataframes["ELEMENTS"][self._dataframes["ELEMENTS"]["PARENT_ID"] == id].index.tolist()
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
