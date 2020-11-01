
import pandas as pd


class DocumentInfo:

    def __init__(self, etree_root):
        self.elements_df = self._get_elements_df(etree_root)
        self.attributes_df = self._get_attributes_df()
        self.elements_df.drop("etree node", axis=1, inplace=True)
        self.root_element = self._get_element(0)

    def get_all_element_tag_names(self, parent_tag_name=None):
        """
        Returns a set of all unique element tag names within the XML document.
        If parent_tag_name is specified, then only the element tags that appear
        with parents of that type are returned.
        """
        tags = set()
        df = self.elements_df
        if parent_tag_name is not None:
            valid_parent_indeces = df[df["tag name"] == parent_tag_name].index
            for i in valid_parent_indeces:
                tags.update(df[df["parent index"] == i]["tag name"].tolist())
        else:
            tags.update(df["tag name"].tolist())
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
                element_index = attribute_row["element index"]
                element = self.elements_df.iloc[element_index]
                if element["tag name"] == element_tag_name:
                    attributes.add(name)
            else:
                attributes.add(name)
        return attributes

    def get_elements_grouped_by_attribute(self, attribute):
        """
        Returns a dictionary of element dataframes indexed by all unique
        values of the specified attribute.
        """
        grouped_elements = {}
        attribute_instances = self.attributes_df[self.attributes_df["name"] == attribute]
        attribute_values = set(attribute_instances["value"].tolist())
        for val in attribute_values:
            element_indeces = attribute_instances[attribute_instances["value"] == val]["element index"]
            elements = self.elements_df.iloc[element_indeces]
            grouped_elements[val] = elements
        return grouped_elements

    def _get_elements_df(self, etree_root):

        df = pd.DataFrame(columns=["etree node", "tag name", "content", "parent index"])

        for node in etree_root.iter():
            df_row_dict = {
                "etree node": node,
                "tag name": node.tag,
                "content": node.text.strip() if node.text is not None and not node.text.isspace() else None,
                "parent index": None
            }
            df = df.append(df_row_dict, ignore_index=True)

        df["parent index"] = df["etree node"].apply(lambda node: self._get_parent_index_from_etree_node(df, node)) \
                                             .astype("Int64")

        return df

    def _get_attributes_df(self):

        df = pd.DataFrame(columns=["name", "value", "element index"])

        for element_index, element_row in self.elements_df.iterrows():
            for name, value in element_row["etree node"].attrib.items():
                df_row_dict = {
                    "name": name,
                    "value": value,
                    "element index": element_index
                }
                df = df.append(df_row_dict, ignore_index=True)

        return df

    def _get_parent_index_from_etree_node(self, df, node):
        is_parent_node = df["etree node"] == node.getparent()
        parent_indeces = df.index[is_parent_node].tolist()
        num_parents = len(parent_indeces)
        if num_parents == 0:
            return None
        elif num_parents == 1:
            return int(parent_indeces[0])
        else:
            raise Exception(f"Found more than one parent for node: {etree.tostring(node)}")

    def _get_element(self, id):
        if id is None:
            return None
        element_row = self.elements_df.iloc[id]
        name = element_row["tag name"]
        content = element_row["content"]
        attribute_rows = self.attributes_df[self.attributes_df["element index"] == id]
        attributes = {row["name"]: row["value"] for i, row in attribute_rows.iterrows()}
        child_indeces = self.elements_df[self.elements_df["parent index"] == id].index.tolist()
        children = [self._get_element(child_id) for child_id in child_indeces]
        element = ElementInfo(id, name, attributes, children)
        for child in element.children():
            child.set_parent(element)
        return element


class ElementInfo:

    def __init__(self, id, name, attributes, children):
        self._id = id
        self._name = name
        self._attributes = attributes
        self._parent = None
        self._children = children

    def id(self):
        return self._id

    def name(self):
        return self._name

    def attributes(self):
        return self._attributes

    def parent(self):
        return self._parent

    def set_parent(self, parent):
        self._parent = parent

    def children(self):
        return self._children

    def has_children(self):
        children = self.children()
        return children is not None and len(children) > 0

    def get_tag_string(self):
        s = f"<{self.name()}"
        if len(self.attributes().keys()) > 0:
            for attribute, value in self.attributes().items():
                s += f' {attribute}="{value}"'
            s += " "
        if self.has_children():
            s += ">"
        else:
            s += "/>"
        return s

    def __str__(self):
        s = self.get_tag_string()
        if self.has_children():
            for child in self.children():
                s += f"\n\t{child.get_tag_string()}"
            s += f"\n</{self.name()}>"
        return s
