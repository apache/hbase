When in the hubspot-2.6 branch, or sub-branches of that branch, use mvn11 instead of mvn, and use the hadoop-3.0 maven profile

When in the master branch, or sub-branches of that branch, use mvn17 instead of mvn, and do not use the hadoop-3.0 maven profile.

Always run the spotless:apply maven target after making changes.
