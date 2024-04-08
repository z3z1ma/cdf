# cdf

CDF (Continuous Data Framework) is an integrated framework designed to manage
data across the entire lifecycle, from ingestion through transformation to
publishing. It is built on top of two open-source projects, `sqlmesh` and
`dlt`, providing a unified interface for complex data operations. CDF
simplifies data engineering workflows, offering scalable solutions from small
to large projects through an opinionated project structure that supports both
multi-workspace and single-workspace layouts. We place a heavy emphasis on the
inner loop of data engineering. We believe that the most important part of data
engineering is the ability to rapidly iterate on the data, and we have designed
CDF to make that as easy as possible. We achieve this through a combination of
dlt's simplicity in authoring pipelines with dynamic parameterization of sinks
and developer utilities such as `head` and `discover`. We streamline the
process of scaffolding out new components and view the idea of a workspace as
something that is full of business-specific components. Pipelines,
transformations, publishers, scripts, and notebooks. Spend less time on
boilerplate, less time figuring out how to consolidate your custom code into
perfect collections of software engineering best practices, and spend much more
time on point solutions that solve your business problems. Thats the benefit of
opinionation. And we offer it in a way that is flexible and extensible.

