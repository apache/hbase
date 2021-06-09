# hubspot-client-bundles

Bundles up the hbase client in a way that is most friendly to the hubspot dependency trees

## Why?

HBase provides some shaded artifacts, but they don't really work for us for two reasons:

1. We have little control over what's included in them, so the jars end up being unnecessarily fat and/or leaking dependencies we don't want.
2. The shaded artifacts have significant class overlaps because one is a superset of the other. This bloats our classpath and also makes `mvn dependency:analyze` complain. This can be a cause for the classic flappy "Unused declared"/"Used undeclared" dependency issue.

One option would be to fix those existing artifacts to work how we'd like. I tried that in hbase2, but it was very complicated without fully redoing how the shading works. Rather than maintain a large rewrite of those poms, I'd rather start fresh with our artifacts. This also will give us greater flexibility in the future for changing the includes/excludes as we see fit.

## Why here?

The other design choice here was to include these artifacts in this repo as opposed to a separate repo. One pain point with developing on hbase has been the number of repos necessary to develop and/or test any change -- the client fork has historically had 2 branches (staging and master) and similar for hbase-shading. In order to get a branch out there for testing you need to modify two repos. Iterating on those branches is annoying because builds are not automatically in-sync.

Putting the bundling here makes it part of the build, so we automatically have client artifacts created for every branch.

One new guiding principle of our forking strategy is to minimize the number of customizations in our forks, instead aiming to get things upstreamed. The goal is to eliminate the tech debt inherent in having to re-analyze, copy patches, handle merge conflicts, etc, every time we upgrade. This module is an omission to that rule -- regardless of where it lives, we will want to be cognizant of dependency changes in new releases. Putting it here gives us the option to bake that process directly into our build and introduces no potential for merge conflicts because it's entirely isolated in a new module.

## How it works

These artifacts are produced with the usual maven-shade-plugin. Some understanding of that plugin is helpful, but I wanted to give a little clarity on a few techniques used.

In general our goal with shading is to control two things:

- Which classes end up in the jar, and the fully qualified class names (i.e. including package) of those classes.
- Which dependencies are exposed in the resulting pom.

At a very high level, the shade plugin does the following:

1. Collect all the dependencies in your pom.xml, including transitive dependencies. It's worth noting that this flattens your dependency tree, so if your project A previously depended on project B which depended on project C, your project A now directly depends on B and C.
2. Include any selected dependencies (via artifactSet) directly into your jar by copying the class files in.
3. Rewrite those class packages and imports, if configured via relocations.
4. Write a new dependency-reduced-pom.xml, which only includes the dependencies that weren't included in the jar. This pom becomes the new pom for your artifact.

In terms of our two goals, choosing which classes end up in the jar is easy via artifactSet. Controlling which dependencies end up in your final pom is a lot trickier:

- **Exclusions** - Since the shade plugin starts with your initial dependencies, you can eliminate transitive dependencies by excluding them from your direct dependencies. This is effective but typically involves needing to apply those same exclusions to all direct dependencies, because the ones you're trying to exclude will often come from multiple.
- **Marking a dependency as scope provided** - The shade plugin seems to ignore scope provided dependencies, as well as all of their transitive dependencies (as long as they aren't converted to compile scope by some other dependency). This sometimes doesn't work and seems kind of magic, so might make sense to only use for cases where your jar actually provides that dependency.
- **Inclusion in the jar** - Any dependencies included in the jar will be removed from the resulting pom. In general if you include something in the jar, it should be relocated or filtered. Otherwise, you run the risk of duplicate class conflicts. You can include something in the jar and then filter out all classes, which sort of wipes it out. But it requires configuring in multiple places and is again sort of magic, so another last resort.

My strategy has evolved here over time since none of these are perfect and there's no easy answer as far as I can tell. But I've listed the above in approximately the order
I chose to solve each dependency. So I mostly preferred exclusions here, then marked some stuff as scope provided, and mostly didn't use the last strategy.

## How to make changes

In general the best way I've found to iterate here is:

1. Create a simple downstream project which depends on one or both of these bundles
2. Run `mvn dependency:list -DoutputFile=dependencies.out` to see a full list of dependencies
3. You can pass that through something like `cat dependencies.out | sed -E -e 's/^ +//' | sed -E -e 's/:(compile|runtime|provided|test).*/:\1/' | sed -E -e 's/:(compile|runtime)$/:compile/' | sort | uniq > dependencies.sorted` to get a file that can be compared with another such-processed file
4. Make the change you want in the bundle, then `mvn clean install`
5. Re-run steps 2 and 3, outputting to a new file
6. Run `comm -13 first second` to see what might be newly added after your change, or `comm -23` to see what might have been removed
7. If trying to track a specific dependency from the list, go back here and run `mvn dependency:tree -Dincludes=<coordinates>`. This might show you what dependency you need to add an exclusion to

This ends up being pretty iterative and trial/error, but can eventually get to a jar which has what you want (and doesn't what you don't).
