
import pandas as pd
from dataframe_sql import register_temp_table, query

class DocumentInfo:

    def __init__(self, etree_root):
        self.elements_df = self._get_elements_df(etree_root)
        self.attributes_df = self._get_attributes_df()
        self.elements_df.drop("etree_node", axis=1, inplace=True)
        register_temp_table(self.elements_df, "elements")
        register_temp_table(self.attributes_df, "attributes")

        # self.root_element = self._get_element(0)

    def get_all_element_tag_names(self, parent_tag_name=None):
        """
        Returns a set of all unique element tag names within the XML document.
        If parent_tag_name is specified, then only the element tags that appear
        with parents of that type are returned.
        """
        tags = set()
        df = self.elements_df
        if parent_tag_name is not None:
            valid_parent_indeces = df[df["tag_name"] == parent_tag_name].index
            for i in valid_parent_indeces:
                tags.update(df[df["parent_id"] == i]["tag_name"].tolist())
        else:
            tags.update(df["tag_name"].tolist())
        return tags

    def get_all_attribute_names(self, element_tag_name=None):
        """
        Returns a set of all unique attribute names within the XML document.
        If element_tag_name is specified, then only the attributes that appear
        in elements of that type are returned.
        """
        attributes = set()
        for index, attribute_row in self.attributes_df.iterrows():
            name = attribute_row["name"]
            if element_tag_name is not None:
                element_index = attribute_row["element_id"]
                element = self.elements_df.iloc[element_index]
                if element["tag_name"] == element_tag_name:
                    attributes.add(name)
            else:
                attributes.add(name)
        return attributes

    def get_elements_info_grouped_by_attribute(self, attribute, include_single_instances=False,
                                               show_only_unique_tag_names=False):
        """
        Returns a dictionary that maps unique values of the specified attribute
        to a list of 'tag_name' and 'id' values for elements containing an
        attribute with that value.

        By default, any attribute values that correspond to a single element
        instance are not returned. To include these, set the include_single_instances
        parameter to True.

        If show_only_unique_tag_names is set to True, then the lists corresponding
        to each unique value of the specified attribute will contain only the unique
        tag_names among their element instances, instead of the 'tag_name' and 'id'
        of each instance.
        """
        grouped_elements_info = {}
        attribute_instances = self.attributes_df[self.attributes_df["name"] == attribute]
        attribute_values = set(attribute_instances["value"].tolist())

        for val in attribute_values:
            element_indeces = attribute_instances[attribute_instances["value"] == val]["element_id"].tolist()
            if len(element_indeces) == 1 and not include_single_instances:
                continue

            elements_info_list = []
            for element_index in element_indeces:
                elements_info_list.append({
                    "tag_name": self.elements_df.iloc[element_index]["tag_name"],
                    "id": element_index
                })

            if show_only_unique_tag_names:
                elements_info_list = list(set([element_info["tag_name"] for element_info in elements_info_list]))

            grouped_elements_info[val] = elements_info_list
        return grouped_elements_info

    def _get_elements_df(self, etree_root):

        df = pd.DataFrame(columns=["etree_node", "tag_name", "content", "parent_id"])

        for node in etree_root.iter():
            df_row_dict = {
                "etree_node": node,
                "tag_name": node.tag,
                "content": node.text.strip() if node.text is not None and not node.text.isspace() else None,
                "parent_id": None
            }
            df = df.append(df_row_dict, ignore_index=True)

        df["parent_id"] = df["etree_node"].apply(lambda node: self._get_parent_index_from_etree_node(df, node)) \
                                             .astype("Int64")

        df["id"] = df.index
        return df

    def _get_attributes_df(self):

        df = pd.DataFrame(columns=["name", "value", "element_id"])

        for element_index, element_row in self.elements_df.iterrows():
            for name, value in element_row["etree_node"].attrib.items():
                df_row_dict = {
                    "name": name,
                    "value": value,
                    "element_id": element_index
                }
                df = df.append(df_row_dict, ignore_index=True)

        df["id"] = df.index
        return df

    def _get_parent_index_from_etree_node(self, df, node):
        is_parent_node = df["etree_node"] == node.getparent()
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
    #     element_row = self.elements_df.iloc[id]
    #     name = element_row["tag_name"]
    #     content = element_row["content"]
    #     attribute_rows = self.attributes_df[self.attributes_df["element_id"] == id]
    #     attributes = {row["name"]: row["value"] for i, row in attribute_rows.iterrows()}
    #     child_indeces = self.elements_df[self.elements_df["parent_id"] == id].index.tolist()
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
#                 s += f' {attribute}="{value}"'
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
