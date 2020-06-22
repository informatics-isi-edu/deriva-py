# Calling deriva-py from R

The [reticulate](https://rstudio.github.io/reticulate/) package can be used to call deriva-py functions from R. This writeup assumes familiarity with R and with the python-based examples in the deriva-py datapath tutorials.

Then, in R, install reticulate:
```
install.packages("reticulate")
```

You can then import Python packages into R:
```
library(reticulate)
deriva.core <- import("deriva.core")
```

Once deriva.core has been imported, we can call deriva-py functions in much the same way we'd call them from Python, keeping a few things in mind:
- We'll need to make some simple syntax changes (R uses `<-`, not `=` for assignment and `$` instead of `.` for a path separator.
- The reticulate library translates certain Python datatypes (data frames, scalars, etc.) into corresponding R types (see [the [reticulate documentation](https://rstudio.github.io/reticulate/) for details). Other python data types are opaque to R but can be used in calls to python functions.
- Datapath filter operations that use overloaded operators must be written with a different syntax in R.

For example, here's some python code (copied from the datapath tutorial) to initiate a connection to a deriva server and get a datapath object corresponding to the tale `isa.dataset` on the host `www.facebase.org`:
```
import deriva.core
protocol = 'https'
hostname = 'www.facebase.org'
catalog_number = 1
credential = None

# If you need to authenticate, use Deriva Auth agent and get the credential
# credential = get_credential(hostname)

catalog = deriva.core.ErmrestCatalog(protocol, hostname, catalog_number, credential)

# Get the path builder interface for this catalog
pb = catalog.getPathBuilder()

# Get some local variable handles to tables for convenience
dataset = pb.isa.dataset
```

and here's the same code in R:
```
library(reticulate)
deriva.core <- import("deriva.core")
protocol <- 'https'
hostname <- 'www.facebase.org'
catalog_number <- 1L
credential <- NULL

# If you need to authenticate, use Deriva Auth agent and get the credential
# credential <- get_credential(hostname)

catalog <- deriva.core$ErmrestCatalog(protocol, hostname, catalog_number, credential)

# Get the path builder interface for this catalog
pb <-catalog$getPathBuilder()

# Get some local variable handles to tables for convenience
dataset <- pb$isa$dataset
```

A couple things to notice: `catalog_number` is set to "1L"; setting it to "1" would lead to it being misinterpreted as a floating-point number, and R uses `NULL` where python uses `None`.

At this point, we could look at the contents of the entire dataset:

```
results <- dataset$entities()
iterate(results, print)
```

But that's a large table, so let's do some filtering. In Python, this is how we'd get a data frame of all dataset
records that were created more recently than November 1, 2018:
```
dataset.filter(dataset.RCT > "2018-11-01").entities()
```

The `>` filter operator won't work in R, so we need to use an alternate syntax:
```
results <- dataset$filter(dataset$RCT$gt("2018-11-01"))$entities()
iterate(results, print)
```

Other filters may be converted similarly.

| Python filter syntax | R filter syntax |
|  --- | --- |
| col == val | col$eq(val) |
| col < val | col$lt(val) |
| col <= val | col$le(val) |
| col > val | col$gt(val) |
| col >= val | col$ge(val) |

Datapath results can also be converted into dataframes via the python `pandas` library.
```
pandas <- import("pandas")
results <- pandas$DataFrame(iterate(dataset$entities()))
```

For more information, see the datapath tutorial and the [reticulate documentation](https://rstudio.github.io/reticulate/index.html).
