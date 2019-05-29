# Sitemap builder library and CLI

## The CLI documentation is [here](../../docs/cli/deriva-sitemap-cli.md).

## SiteMapBuilder class

Typical usage is:

1. Create a SiteMapBuilder class

2. For each table to include in the sitemap, create a table spec and populate it with the table's data. Typically, the creating and populating for a table is done by a single call to add_table_spec.

3. Call write_sitemap to write out the site map

### Examples

In the simplest case, this would create a sitemap with two tables::

```
    sb = SitemapBuilder("https", "myhost.org", 1)
    sb.add_table_spec("schema1", "table1")
    sb.add_table_spec("schema2", "table2")
    sb.write_sitemap(sys.stdout)
```

If you want to include only a subset of rows of a table, you can pass a datapath to add_table_spec::


```
    pb = catalog.getPathBuilder()
    path=pb.schema3.table3.filter(pb.schema3.table3.Species=="Homo sapiens")
    sb.add_table_spec("schema3", "table3", datapath=path)
```

If you want to do something more customizable, you can populate the spec yourself::

```
    rows = do_something_complicated()
    spec = sb.add_table_spec("schema4", "table4", populate=False)
    sb.set_table_spec_data(rows)
    sb.add_fkey_times(spec)
```

Note: if add_table_spec populates the spec for you, it will set the modification time based on the times in the table and on all single-valued incoming foreign keys (because those are the most likely to affect a Chaise record page). If you're populating the spec yourself (i.e., if you called add_table_spec with populate=False), you can call add_fkey_times to make those time adjustments.

### Limitations:

Sitemaps should be no more than 50MB in size and should contain no more than 50,000 URLS (https://www.sitemaps.org/faq.html#faq_sitemap_size), but this class doesn't enforce those limits. (Note: A site can have multiple sitemaps).

A URL element should have no more than 1000 images associated with it (https://support.google.com/webmasters/answer/178636?hl=en), but this class doesn't enforce that limit.

This class assumes that all images in a catalog will have the same license.




