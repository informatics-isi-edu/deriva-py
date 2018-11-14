
# Starting a Deriva-Py Data Management Project

This document provides a best practice for starting a new project with
a new ERMrest catalog and Hatrac object store, using Deriva-py client APIs
to setup the project.

When starting, many projects do not have very complex access-control
policy requirements. However, establishing a few good practices from
the start can help immensely as the project evolves later:

1. Create Globus groups representing access _roles_ in the catalog.
2. Create an initial ERMrest catalog and obtain a Hatrac object storage namespace.
3. Set ERMrest catalog and Hatrac object store policies to grant rights to those groups.
4. Add users to groups based on their expected role in the project.
5. Gain experience and revisit policy design as you go.

## Python Client Bindings

Throughout this document, we give snippets of example Python code
assuming you are using our `derivapy` client library and have already
established active server credentials using the `deriva-auth`
graphical authentication agent. When combining these tools with
project-specific Globus groups, it is important to stop and restart
your `deriva-auth` sessions after your group memberships have changed,
to avoid any confusion with stale membership affecting your subsequent
API requests.

This code establishes the presumed Python environment and all
subsequent Python examples should be read as incrementally extending
this environment in the order presented:

    from deriva.core import DerivaServer, ErmrestCatalog, HatracStore, AttrDict, get_credential
    from deriva.core.ermrest_model import builtin_types, Table, Column, Key, ForeignKey

    # replace this with your real server FQDN
    servername = "yourserver.example.com"
    credentials = get_credential(servername)


## Your Project-Specific Groups

A successful project will require several Globus groups to be
created, each representing a meaningful role.

### Suggested Groups

You may consider starting with a few designated roles in your project.
The _anonymous_ role is not represented by a Globus group, but rather
embodies all clients without authentication status giving them any
other role-based privileges, including real anonymous users without
any login status as well as logged in users who do not belong to any
of your project-designated roles:

| Role        | Purpose |
|-------------|---------|
| admin       | Owns catalog and can manipulate models, policies, and data. |
| curator     | Trusted to create/read/update/delete all data but not model nor policy. |
| writers     | Trusted to create and edit their own records and to read records created by others. |
| readers     | Trusted to read all data in catalog. |
| _anonymous_ | Not allowed to do anything useful. |

NOTE: A small project may skip the "writers" and "readers" groups
during initial work, if all initial project members are equally
trusted to managed and consume project data. The other roles may be
introduced incrementally as the need arises. However, we will give
examples as if these roles are all useful from the start.

### Keeping Track of Group Identifiers

Each group has a unique hexadecimal identifier and corresponds to a
simple role within the project. For use in our code snippets, we want
to keep track of these identifiers:

    # replace these with your real group IDs
    grps = AttrDict({
      "admin":   "https://auth.globus.org/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "curator": "https://auth.globus.org/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "writer":  "https://auth.globus.org/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "reader":  "https://auth.globus.org/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    })

this data structure allows us to access them by shorter role names,
e.g. `grps.admin` when constructing policies later.

## Your First Hatrac Namespace

An adminstrative client requests creation of a top-level namespace in
the Hatrac object store and sets appropriate ACLs to allow subsequent
management of data objects:

    object_store = HatracStore('https', servername, credentials)
    namespace = '/hatrac/project_data'
    object_store.create_namespace(namespace)
    object_store.set_acl(namespace, 'owner', [grps.admin])
    object_store.set_acl(namespace, 'subtree-create', [grps.curator, grps.writer])
    object_store.set_acl(namespace, 'subtree-update', [grps.curator])
    object_store.set_acl(namespace, 'subtree-read', [grps.reader])

These requests can only succeed if the client represented by the
credentials is granted namepsace creation rights on the root namepsace
of the server. Often, a server administrator must assist with this
first step.

The recommended policies above will allow curators and writers to
create new objects, curators to update objects, and readers to read
objects under the common project namespace. General Hatrac behavior
also sets a per-object ownership ACL to the requesting client, so
writers are able to update objects they have created but not those
created by other writers nor curators.

## Your First Catalog

An administrative client requests creation of a new catalog on an
ERMrest server:

    server = DerivaServer('https', servername, credentials)
    catalog = server.create_ermrest_catalog()

This request can only succeed if the client represented by the
credentials is granted catalog creation rights on the server (via an
ACL in the `ermrest_config.json` service configuration file).

### Initial Catalog Content

Starting with a new catalog, your model is _almost_ empty. Every new
catalog already has:

- The administrative client is set as `owner` ACL on the catalog.
- A `public` schema.
- An `ermrest_client` table in the `public` schema, hidden by default.

The administrative client can reconfigure the initial catalog policies
and start creating project-specific model definitions.

You can inspect the `ermrest_client` table definition:

    model = catalog.getCatalogModel()
    client_table = model.schemas["public"].tables["ermrest_client"]
    
    # you can access the column definitions in their defined order in the table
    for column in client_table.column_definitions:
      print((column.name, column.type.typename, column.nullok))
    
    # you can also look up columns by name or by position
    column = client_table.column_definitions[0]
    assert column == client_table.column_definitions[column.name]

The column definitions are summarized in the following table.

| Column Name  | Typename     | Null OK | Description |
|--------------|--------------|---------|-------------|
| RID          | ermrest\_rid | false   | System-managed _record identifier_ |
| RCT          | ermrest\_rct | false   | System-managed _record creation timestamp_ |
| RMT          | ermrest\_rmt | false   | System-managed _record last modification timestamp_ |
| RCB          | ermrest\_rcb | true    | System-managed _record created by_ provenance |
| RMB          | ermrest\_rmb | true    | System-managed _record last modified by_ provenance |
| id           | text         | false   | Authenticated client identifier i.e. `https://auth.globus.org/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| display_name | text         | true    | Human-readable display name of client, if known |
| full_name    | text         | true    | Human-readable full name of client, if known |
| email        | text         | true    | Email address of client, if known |
| client_obj   | jsonb        | false   | JSON object containing all attributes known from identity provider |

The first five columns `RID`, `RCT`, `RMT`, `RCB`, and `RMB` are
ERMrest _system columns_ present in every table. The remaining columns are
specific to the `ermrest_client` table's purpose.

### Your First Catalog Policy

We can configure the catalog representing most of our desired roles:

    # modify local representation of catalog ACL config
    model.acls.update({
      "owner": [grps.admin],
      "insert": [grps.curator, grps.writer],
      "update": [grps.curator],
      "delete": [grps.curator],
      "select": [grps.writer, grps.reader],
      "enumerate": ["*"],
    })
    # apply these local config changes to the server
    model.apply(catalog)

This policy is sufficient to allow initial project work using the
admin, curator, and reader roles. However, to fully enable the less
privileged writer role, you will also need to deploy additional
data-dependent policies on each table where those writers are allowed
to modify their own existing records. The preceding catalog-wide
policy alone will allow them to create records on any table, but only
curators and admins may edit existing records.

The `enumerate` ACL configuration supports our companion ERMresolve
service but also exposes basic model structures to anonymous
clients. Please
see
[further discussion on enumerable models](#enumerable-catalogs-support-ermresolve).

### Static Policy Inheritance

Because ERMrest allows elements of the model to _inherit_ ACLs from
enclosing scopes, a small project can set one catalog-wide ACL
configuration and omit any localized ACLs on specific schemas, tables,
columns, etc.  This provides a consistent data access-control behavior
across the entire catalog.

Later, if certain tables or columns require more sophisticated access
control rules, you can override the catalog-wide ACL and supply a
more specific local policy instead.

### Exposing the ERMrest Client Table

The `ermrest_client` table automatically records each newly signed on
user, providing a useful reference for catalog administrators to map
opaque Globus user identifiers into human-readable user profile information
such as full name, display name, or email addresses.

During catalog creation, this table receives a table-level policy
which sets all table ACLs to the empty list `[]` to suppress any ACLs
inherited from the `public` schema or the catalog as a whole. This
effectively hides the table and its content from all but
administrative clients.

You may consider relaxing this policy to allow your other roles to see
basic user information. However, we recommend that you restrict
general users from accessing the `client_obj` column unless you have
reviewed the available content and your own project's privacy policies
and determine that it is appropriate to share this raw identity
provider information:

    # mutate local configuration of client_table which is part of model
    client_table.acls.update({
      "select": [grps.curator, grps.writer, grps.reader],
    })
    client_table.column_definitions["client_obj"].acls.update({
      "select": [],    
    })
    
    # apply these local changes to server
    model.apply(catalog)

This is particularly useful if you create foreign key relationships
from system-generated provenance columns to the `id` key column of
this table. The `RCB` and `RMB` columns store the opaque identifiers
of clients creating or modifying records, respectively.

## Extending Your Catalog

The ERMrest service provides a set of HTTP operations for managing the
model of a catalog and our Python bindings provide some helper APIs to
perform these tasks.

The preceding code snippets for configuring policies already use some
features specific to managing ACLs. There are also specific API
methods for managing the structure of your model, i.e. adding and
removing tables, columns, and other model elements.

These Python API bindings are relatively simple wrappers for the raw
HTTP methods, using Python data structures to represent document
structures sent over the wire in JSON format. A Python client
programmer may familiarize themselves with the behavior of the
`json.dumps` and `json.loads` serialization/deserialization routines
to understand how Python `dict`, `list`, string, number, and boolean
types are mapped to JSON documents.

As a convenience, we provide some "factory" routines to help construct
documents suitable as input to these model management APIs.

### Adding a Table

When adding a table for our project, we only need to define
project-specific columns while allowing the API to fill in the ERMrest
system columns for us. Here is a trivial "Journal" example where we
simply define a "Notes" column to store formatted markdown text, while
relying on the system columns to provide keys and basic timestamp
metadata:

    model.schemas["public"].create_table(
      Table.define(
        "Journal",
        [
          Column.define(
            "Notes",
            builtin_types["markdown"],
            nullok=False,
            comment="User-provided notes.",
          )
        ],
        comment="A journal of user-provided notes."
      )
    )
    
    # retrieve catalog model again to ensure we reflect latest structural changes
    model = catalog.getCurrentModel()

### Adding an Asset Table

Let's embellish our Journal to allow the user to submit file attachments
linked to their journal entries:

    model.schemas["public"].create_table(
      Table.define(
        "Journal_Attachment",
        [
          Column.define(
            "journal_rid",
            builtin_types["text"],
            nullok=False,
            comment="The journal entry to which this asset is attached."
          ),
          Column.define(
            "url",
            builtin_types["text"],
            nullok=False,
            comment="The URL of the stored attachment."
          ),
          Column.define(
            "length",
            builtin_types["int8"],
            nullok=False,
            comment="The asset length (byte count)."
          ),
          Column.define(
            "md5",
            builtin_types["text"],
            nullok=False,
            comment="The hexadecimal encoded MD5 checksum of the asset."
          ),
          Column.define(
            "content_type",
            builtin_types["text"],
            nullok=True,
            comment="The content-type of the asset."
          ),
          Column.define(
            "file_name",
            builtin_types["text"],
            nullok=True,
            comment="The suggested local filename on client systems."
          ),
        ],
        fkey_defs=[
          ForeignKey.define(
            ["journal_rid"],
            "public",
            "Journal",
            ["RID"],
            on_delete="CASCADE",
            constraint_names=[["public", "Journal_Attachment_journal_rid_fkey"]]
          ),
        ],
        comment="Assets (files) attached to Journal entries.",
      )
    )
    
    # retrieve catalog model again to ensure we reflect latest structural changes
    model = catalog.getCurrentModel()

This example defines a number of standard metadata columns to
represent an uploaded file, as well as a foreign key `journal_rid` linking
each asset to a specific Journal entry.

### Asset Configuration

To get the best experience from the Chaise GUI, we also want to add model
_annotations_ declaring how we prefer to present this table:

    attachment_table = model.schemas["public"].tables["Journal_Attachment"]
    url_column = attachment_table.column_definitions["url"]
    
    url_column.annotations.update({
      "tag:isrd.isi.edu,2017:asset": {
        "filename_column": "file_name",
        "byte_count_column": "length",
        "md5": "md5",
        "url_pattern": "/hatrac/project_data/journal_attachment/{{{journal_rid}}}/{{{_url.md5_hex}}}"
      }
    })
    
    attachment_table.annotations.update({
      "tag:isrd.isi.edu,2015:display": {
        "name_style": { "underline_space": True }
      },
      "tag:isrd.isi.edu,2016:visible-columns": {
        "entry": [
          ["public", "Journal_Attachment_journal_rid_fkey"],
          "url"
        ]
      }
    })
    
    model.apply(catalog)

The first `tag:isrd.isi.edu,2017:asset` annotation tells Chaise to
provide a more specialized asset behavior with upload/download
features when presenting the `url` column:

- Present existing `url` content as a download link.
- Offer a file-chooser to submit new files in data-entry forms.
- Name uploaded assets in the object store based on linked Journal entry RID and asset checksum.
- Store computed asset metadata to additional metadata columns: MD5, length, and original file name.

Without this, Chaise would just present it as a textual column
containing a URL.

The second `tag:isrd.isi.edu,2015:display` annotation simply tells
Chaise that we prefer to display the table name of
`Journal_Attachements` as `Journal Attachments` by rewriting the
underscore as whitespace.

The third `tag:isrd.isi.edu,2016:visible-columns` annotation further
customizes data-entry in Chaise, while using default presentation in
other read-only contexts:

- Instead of display the raw value in `journal_rid`, interpret the
  foreign key linkage and present an item-chooser to link this Journal
  Attachment to an existing Journal record.
- Prompt for `url` input, which will be displayed as a file-chooser
  due to the preceding asset annotation.
- Don't prompt for input on other excluded columns, since they will
  automatically be populated by the Chaise file uploader based on the
  preceding asset annotation.

### Allowing Self-Service Editing by Writers

Our initial catalog policy allows writers to insert new records in our
Journal table, but after that only curators can revise them. If we
wish to enable self-service editing of journal entries by writers, we
need to add a data-dependent policy to the table:

    journal_table = model.schemas["public"].tables["Journal"]
    attachment_table = model.schemas["public"].tables["Journal"]

    # we can re-use this generic policy on any table since they all have an RCB column
    self_service_policy = {
      "self_service": {
        "types": ["update", "delete"],
        "projection": ["RCB"],
        "projection_type": "acl"
      }
    }
    
    journal_table.acl_bindings.update(self_service_policy)
    attachment_table.acl_bindings.update(self_service_policy)
    
    # apply these local config changes to the server
    model.apply(catalog)

This slightly cryptic policy states that ERMrest should project the
`RCB` (aka _row created by_) column and treat it like an ACL; when the
requesting client matches this extra ACL for an existing row, the
client is granted the `update` and `delete` access rights which they
otherwise would not have.

In practice, this means that after a client with writer role inserts a
Journal entry or Journal attachment, their client ID will be listed as
the `RCB` and they will be permitted to make updates to or delete
their own journal entries.

### Preventing Attachments by Unrelated Writers

The policy so far is a little bit trusting. It does not prevent one
writer from introducing attachments to a different writer's Journal
entries. To restrict this, we can leverage a more subtle ERMrest
feature which allows policies on foreign keys.

Rather than determining row permissions based on row-specific content
like `RCB` of the row being modified, such policies can restrict the
_expression_ of a foreign key value in the controlled foreign key
column based on row-specific content of the referenced entity. In
this case, we can make sure writers can only link an attachment to
a Journal record if they are the creator of that Journal entry:

    journal_fkey = attachment_table.foreign_keys[
      ("public", "Journal_Attachment_journal_rid_fkey")
    ]
    
    journal_fkey.acls.update({
      "insert": [grps.curator],
      "update": [grps.curator],
    })
    
    journal_fkey.acl_bindings.update({
      "self_linkage": {
        "types": ["insert", "update"],
        "projection": ["RCB"],
        "projection_type": "acl",
      }
    })

The preceding static ACLs overrides the ERMrest default behavior
assuming an ACL of `["*"]` to allow unconstrained use of foreign key
values; in this case, we still want to allow curators to adjust
attachment linkage regardless of whether they created a Journal entry
or not.

The dynamic ACL binding is much like the self-service binding
introduced earlier. There are two subtle differences:

1. The projected `RCB` column comes from the referred row in the
   Journal table rather than the Journal\_Attachment table row being
   modified.
2. Because the projected row already exists, we can set a policy for
   the `insert` access type, i.e. while creating a new
   Journal\_Attachment record. Regular table (and column) ACL bindings
   cannot control `insert` access since there is no existing row to
   consult when determining access privileges.

### Pitfall of Foreign Key Policies

While this policy mechanism is useful and important in some scenarios,
there is a potential user-experience problem.  The Chaise GUI does
not understand the restriction and will offer the user choices which
will subsequently fail when submitted to the server.

For now, use of such policies should include user training so they
understand the policy of the project. Eventually, we hope to make the
policy visible in some way to Chaise so that it can automatically
restrict the offered choices to those which may succeed.

### Adding a Column

Our trivial Journal example only has the system-managed timestamp
information for when an entry is created. There is no option for an
ERMrest client to override and "backdate" these timestamps if they
wish to enter past information that was collected offline. We can add
a Date column to model a user-provided timestamp:

    journal_table.add_column(
      "Date",
      builtin_types["timestamptz"],
      nullok=False,
      comment="User-provided timestamp."
    )
    # retrieve catalog model again to ensure we reflect latest structural changes
    model = catalog.getCurrentModel()

With no other special configuration, this column will experience the
same policy as the existing Notes column.

### Removing a Column

Assuming our users never really enter offline journal entries, they may report
that the new Date column is cumbersome.  We can remove it from the model:

    journal_table.column_definitions["Date"].delete()
    # retrieve catalog model again to ensure we reflect latest structural changes
    model = catalog.getCurrentModel()

### Removing a Table

In the same manner, we can prune an entire table when we realize users do not
actually find it useful:

    model.schemas["public"].tables["Journal"].delete()
    # retrieve catalog model again to ensure we reflect latest structural changes
    model = catalog.getCurrentModel()

This request will actually fail unless we first remove the dependent
Journal\_Attachment table (or at least the foreign key constraint
linking the two):

    model.schemas["public"].tables["Journal_Attachment"].delete()
    model.schemas["public"].tables["Journal"].delete()
    # retrieve catalog model again to ensure we reflect latest structural changes
    model = catalog.getCurrentModel()

Note, even though we deleted the attachment table, all existing assets
in the Hatrac object store will remain.

A project data administrator needs to consider data retention policies
when deciding whether they should periodically search for
abandoned/orphaned objects or retain them in case there are external
URL references to these objects outside the ERMrest metadata catalog.

### Other Model Changes...?

## Further Discussion

Here we discuss related topics and clarify the background for certain
recommendations.

### Curators versus Writers

We suggest starting with the "curator" role.  In a simple project,
this may be the only data-writing role you need. As project needs
evolve, you may introduce one or more other less-privileged groups who
have some write privileges on specific bits of the catalog. However,
you should still retain a curator role given more holistic privileges
over the entire catalog, as embodied in this initial policy.

In our example configuration above, we designated one "writer"
role. However, many projects may find that they have more than one
class of writer, each with different granted privileges. For example,
some tables may be written by one subgroup of users but not
others. Or, some tables may have records associated with individual
project sites, and a per-site writer group should be able to modify
rows associated with their own site regardless of whether they were
the user creating the record.

### Anonymous Access

Our proposed policy configuration requires all users to be added to at
least one group and to sign in before being able to access catalog
content.  Some projects reach a point where they wish to expose some
of their data to the public, without requiring users to obtain
accounts, be enrolled in groups, nor even sign in.

The `"*"` wildcard in an ACL grants permission to anonymous clients.
So, projects can revise their policies to grant additional rights to
anonymous clients as projects evolve.  However, the policy design
needed to provide appropriate anonymous access is beyond the scope of
this initial policy sketch. Oftentimes, a project's intention to make
content visible to the public is actually more nuanced than simply
extending a blanket `select` privilege to all comers. For example,
they may want to track a curation state or "release status" and only
expose records to the public when they have been reviewed and marked
as publication-ready.

### Extending ERMrest Client Table Definition

More sophisticated projects may wish to extend the ERMrest client
table with more localized user information. This is allowed, but you
should remember that ERMrest automatically maintains the core client
information columns and will update records on each return of the
client. Thus, attempting to amend user profile information would be
fruitless as it would be reset to the values provided by the
authentication system. There are two reasonable approaches to
extending the table:

1. Add columns edited by curators (or by users themselves with
   appropriate self-service policies in place).
2. Link the client table to other project tables via foreign keys or
   associations.

### Enumerable Catalogs support ERMresolve

The `enumerate` ACL means that the catalog can be detected even by
anonymous clients. This is useful to allow our companion ERMresolve
service to easily support resolution of short record IDs against a
catalog:

- Ermresolve URL: /id/xyz
- ERMrest resolution support URL: /ermrest/catalog/1/entity_rid/xyz
- ERMrest record URL: /ermrest/catalog/1/entity/Schema1:Table1/RID=xyz
- Chaise record URL: /chaise/record/#1/Schema1:Table1/RID=xyz

During normal operation, the resolver probes the resolution support
URL which is governed by the `enumerate` ACL. On successful
resolution, the resolver redirects HTTP clients to Chaise or ERMrest
record URLs. The client encounters normal data access control
enforcement when they attempt to access the real record.

However, this resolution mechanism also means that anonymous clients
may be able to discover the model definitions in your catalog. If you
plan to use the resolver service you should avoid using model element
names or structures which you would consider too sensitive to reveal
to anonymous clients.  Conversely, if you have sensitive model
structures you can render your system safe by setting the `enumerate`
ACL to the empty list `[]` and hence disabling ERMresolve.

If you think you have a need to both hide sensitive parts of your
model and use the resolution service, please reach out to us so we can
better understand your use case requirements and provide better advice
or possibly expand the capabilities of ERMrest to cover your scenario!


