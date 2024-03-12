from xml.dom.minidom import getDOMImplementation
from deriva.core import DerivaServer, get_credential, BaseCLI
import time
import sys


class SitemapBuilder:

    """Class to build sitemaps from deriva catalogs.

      - Typical usage is:

         1. Create a SiteMapBuilder class
         2. For each table to include in the sitemap, create a table spec and populate it with the
            table's data. Typically, the creating and populating for a table is done by a single
            call to add_table_spec.
         3. Call write_sitemap to write out the site map

      - In the simplest case, this would create a sitemap with two tables::

            sb = SitemapBuilder("https", "myhost.org", 1)
            sb.add_table_spec("schema1", "table1")
            sb.add_table_spec("schema2", "table2")
            sb.write_sitemap(sys.stdout)

      - If you want to include only a subset of rows of a table, you can pass a datapath to add_table_spec::

           pb = catalog.getPathBuilder()
           path=pb.schema3.table3.filter(pb.schema3.table3.Species=="Homo sapiens")
           sb.add_table_spec("schema3", "table3", datapath=path)

      - If you want to do something more customizable, you can populate the spec yourself::

           rows = do_something_complicated()
           spec = sb.add_table_spec("schema4", "table4", populate=False)
           sb.set_table_spec_data(rows)
           sb.add_fkey_times(spec)

        Note: if add_table_spec populates the spec for you, it will set the modification time based on
        the times in the table and on all single-valued incoming foreign keys (because those are the
        most likely to affect a Chaise record page). If you're populating the spec yourself (i.e., if
        you called add_table_spec with populate=False), you can call add_fkey_times to make those time
        adjustments.

       Limitations:
         Sitemaps should be no more than 50MB in size and should contain no more
         than 50,000 URLS (https://www.sitemaps.org/faq.html#faq_sitemap_size),
         but this class doesn't enforce those limits. (Note: A site can have
         multiple sitemaps).

         A URL element should have no more than 1000 images associated with it
         (https://support.google.com/webmasters/answer/178636?hl=en), but this
         class doesn't enforce that limit.

         This class assumes that all images in a catalog will have the same
         license.
    """

    def __init__(self, protocol, host, catalog_id, license_url=None):
        """ Creates the sitemap object
        :param protocl: protocol (e.g., https)
        :param host: the host name
        :param catalog_id: the catalog id
        :param license_url: the url to the license used for images in this catalog
        """

        self.protocol = protocol
        self.host = host
        self.catalog_id = catalog_id
        self.license_url = license_url
        self._create_xml_tree()
        # Don't authenticate - we want only publicly-available content in the index
        self.server = DerivaServer(protocol, host)
        self.catalog = self.server.connect_ermrest(catalog_id)
        self.model = self.catalog.getCatalogModel()
        self.pb = self.catalog.getPathBuilder()
        self.table_specs = []

    def add_table_spec(self, schema, table, datapath=None, populate=True, priority=None):
        """Create a table spec and add it to the sitemap.

        :param schema: the name of the table's schema
        :param table: the table name
        :param datapath: a datapath to use to populate the table (regardless of the value
                         of "populate")
        :param populate: an indication of whether or not the spec should be populated.
                         If populate==True, the table spec's data will be populated with the results
                         from the query specified by "datapath" (if non-None) or all the rows of the table.
                         If populate==False (and datapath is not set), the table spec's data will
                         need to be set via a call to set_table_spec_data()
        :param priority: the priority to assign to sitemap entries created from this table
                         (see https://www.sitemaps.org/protocol.html for a discussion of priorities)

        """
        spec = {
            "table": self.get_table(schema, table),
            "incoming_fkeys": self.model.table(schema, table).referenced_by,
            "url":
            "{protocol}://{host}/chaise/record/?{catalog_id}/{schema}:{table}/RID="
            .format(
                protocol=self.protocol,
                host=self.host,
                catalog_id=self.catalog_id,
                schema=schema,
                table=table)
        }
        if priority is not None:
            spec["priority"] = str(priority)

        if populate or datapath is not None:
            self.populate_spec(spec, datapath)

        self.table_specs.append(spec)
        return spec

    def write_sitemap(self, file):
        """Write out the sitemap

           :param file: The file to write to
        """

        for spec in self.table_specs:
            self._add_spec_records(spec)

        self.doc.writexml(file, encoding="UTF-8", newl="\n", addindent="  ")

    def _create_xml_tree(self):
        """Create empty xml tree"""
        impl = getDOMImplementation()
        self.doc = impl.createDocument(
            "http://www.sitemaps.org/schemas/sitemap/0.9", "urlset", None)
        self.tree = self.doc.documentElement
        self.tree.setAttributeNS("xmlns", "xsi",
                                 "http://www.w3.org/2001/XMLSchema-instance")
        self.tree.setAttributeNS(
            "xsi", "schemaLocation",
            "http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd")
        self.tree.setAttribute(
            "xmlns:image", "http://www.google.com/schemas/sitemap-image/1.1")
        self.tree.setAttribute("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9")

    def _add_spec_records(self, spec):
        """Add records to xml doc"""
        rows = spec.get("data")
        basepath = spec.get("url")
        priority = spec.get("priority")

        for rec in rows:
            url_node = self.doc.createElement("url")
            loc_node = self.doc.createElement("loc")
            loc_node.appendChild(
                self.doc.createTextNode(
                    "{base}{rid}".format(base=basepath, rid=rec.get("RID"))))
            url_node.appendChild(loc_node)
            lastmod_node = self.doc.createElement("lastmod")
            lastmod_node.appendChild(self.doc.createTextNode(rec.get("RMT")))
            url_node.appendChild(lastmod_node)
            if priority is not None:
                priority_node = self.doc.createElement("priority")
                priority_node.appendChild(self.doc.createTextNode(priority))
                url_node.appendChild(priority_node)
            images = rec.get("images")
            # the default dom implementation doesn't include namespaces in output,
            # so use "image:" as part of the element name
            if images is not None:
                for image in images:
                    if image.get("image_url") is not None:
                        image_node = self.doc.createElementNS(
                            "http://www.google.com/schemas/sitemap-image/1.1",
                            "image:image")
                        image_loc_node = self.doc.createElementNS(
                            "http://www.google.com/schemas/sitemap-image/1.1",
                            "image:loc")
                        image_loc_node.appendChild(
                            self.doc.createTextNode(image.get("image_url")))
                        image_node.appendChild(image_loc_node)
                        if image.get("image_caption") is not None:
                            image_caption_node = self.doc.createElementNS(
                                "http://www.google.com/schemas/sitemap-image/1.1",
                                "image:caption")
                            image_caption_node.appendChild(
                                self.doc.createTextNode(image.get("image_caption")))
                            image_node.appendChild(image_caption_node)
                        if image.get("image_title") is not None:
                            image_title_node = self.doc.createElementNS(
                                "http://www.google.com/schemas/sitemap-image/1.1",
                                "image:title")
                            image_title_node.appendChild(
                                self.doc.createTextNode(image.get("image_title")))
                            image_node.appendChild(image_title_node)
                        if self.license_url is not None:
                            image_license_node = self.doc.createElementNS(
                                "http://www.google.com/schemas/sitemap-image/1.1",
                                "image:license")
                            image_license_node.appendChild(
                                self.doc.createTextNode(self.license_url))
                            image_node.appendChild(image_license_node)
                        url_node.appendChild(image_node)
            self.tree.appendChild(url_node)

    def populate_spec(self, spec, datapath):
        """Add data to a spec

        :param spec: the spec to populate
        :param datapath: optional datapth to use (if None, all records
                         from the spec table will be incuded)
        """
        if datapath is None:
            rows = spec["table"].entities()
        else:
            rows = datapath.entities()

        spec["data"] = rows
        self.add_fkey_times(spec)

    def add_fkey_times(self, spec):
        """Replace the RMT in each row with the greatest value of
           the row's RMT and the RMTs of all single-column-fkey-linked
           tables.

           :param spec: a populated spec
        """
        rows = spec["data"]
        ref_table = spec["table"]
        fkeys = spec["incoming_fkeys"]
        rdict = dict()
        if len(fkeys) > 0:
            for r in rows:
                rdict[r.get("RID")] = {"row": r}
        for fkey in fkeys:
            if len(fkey.referenced_columns) == 1:
                # Assume that if a related table is visible in chaise,
                # a single-valued fkey exists.
                self._apply_fkey(ref_table, rdict, fkey)

    def _apply_fkey(self, ref_table, rdict, fkey):
        fkc = fkey.foreign_key_columns[0]
        rc = fkey.referenced_columns[0]
        fk_table = self.get_table(fkc["schema_name"], fkc["table_name"])
        fk_col = self.get_column(fk_table, fkc["column_name"])
        ref_col = self.get_column(ref_table, rc["column_name"])
        datapath = ref_table.alias("ref")\
            .link(fk_table.alias("fk"), on=(ref_col == fk_col))
        fk_rows = datapath.attributes(datapath.ref.RID, datapath.fk.RMT)
        for fk_row in fk_rows:
            entry = rdict.get(fk_row["RID"])
            if entry is not None:
                fk_time = self.ermrest_time_to_float(fk_row["RMT"])
                row = entry.get("row")
                entry_time = entry.get("rmt_float")
                if entry_time is None:
                    entry_time = self.ermrest_time_to_float(row["RMT"])
                if fk_time > entry_time:
                    row["RMT"] = fk_row["RMT"]
                    entry["rmt_float"] = fk_time
                else:
                    entry["rmt_float"] = entry_time

    @staticmethod
    def ermrest_time_to_float(ermrest_time):
        """Converts a time string as returned by ermrest to a floating-point number
        """
        return(time.mktime(time.strptime(ermrest_time, "%Y-%m-%dT%H:%M:%S.%f%z")))

    def get_table(self, sname, tname):
        """Gets a datapath table
        """
        return self.pb.schemas.get(sname).tables.get(tname)

    def get_column(self, table, column_name):
        """Gets a datapath column
        """
        return table.column_definitions.get(column_name)

    def set_table_spec_data(self, spec, rows):
        """Add data to a table spec. This should be used if you want to associate
           image data with the rows of this table, or if you want to include
           only a subset of the table's rows in the sitemap. Otherwise, you can
           just use populate=True in your call to add_table_spec().

        :param spec: a table spec returned by add_table_spec()
        :param rows: an array of dictionary objects corresponding to the rows
           of the table. Each row MUST have RID and RMT elements and MAY have
           an "images" element. If "images" is present, each entry should have:
              "image_url" : the absolute URL of the image (jpeg, etc.)
              "image_caption" : a caption for the image (optional)
              "image_title" : a title for the image (optional)

           for example::

              [
                 {
                    "RID": '1-2345',
                    "RMT": '018-12-03T18:29:38.348231-08:00'},
                 {
                    "RID": '1-2346',
                    "RMT": '018-12-03T18:29:38.348231-08:00',
                    "images": [
                        {
                           "image_url": "https://myhost.org/hatrac/images/image1.jpg",
                           "image_title": "MyProject Image 2-3456: An Awesome Image",
                           "image_caption": "This is an awesome image"
                        },
                        {
                           "image_url": "https://myhost.org/hatrac/images/image2.jpg",
                        }
                    ]
                 }
              ]
        """
        spec["data"] = rows
