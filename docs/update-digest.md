# Update digest

Moved out of the README on 2026-11-15, unchanged apart from relative links.  These are the per-update summary paragraphs that the README's Analysis Results section carried, in the order they were written, oldest first; a new paragraph is appended after the last one.  The full narrative of every update is in [RESEARCH-LOG.md](../RESEARCH-LOG.md).

Phase 1 indicators (b-value, Kp, low-res TEC) were all negative after bias correction. Phase 2 found 3 physics-based signals that survived aftershock isolation and prospective testing. **Phase 4 forward-looking evaluation achieved 62.5% precision (7.8x gain) by combining ETAS residual + cumulative CFS + foreshock alarms.**

The research narrative -- every pre-registered round, its floors, its verdict and the corrections that followed -- lives in [RESEARCH-LOG.md](../RESEARCH-LOG.md), newest last, with an index at the top.  It was kept inline here until 2026-08-23 and was moved unchanged.

**Where the research stands (2026-09-05).** Since 2026-09-02 the question being worked on is
the *isolated mainshock* -- a shock that is not an aftershock of anything larger -- because a
measurement that month showed the pooled figure is carried by aftershock sequences. On the
scored rows the arm beats its own climatology by +0.018 per window on aftershock-driven
positives (39% of the positives) but by only +0.007 on isolated mainshocks (61%), and by +0.010
on the stricter 11% that had no M>=4 within 50 km in the prior year -- that last figure being
the one that does not pass its own interval. The most recent work runs in a causally-selected
global arena (2,280 one-degree cells that were already active in the first fifteen years, scored
from year twenty, 9,993,240 rows over 36 windows), judged against permutation floors under
contracts frozen before each run. Detection completeness in the global catalogue is not uniform
in space or time, and the arena's seed years are its least complete.

Two of the floored verdicts there. Aiming a second factor at *large* mainshocks rather than at
mainshocks is worth +0.0027 per window at magnitude 6.0 against a floor whose five worlds average
+0.0002 (round 201). Replacing that fitted factor with a per-cell rate counted directly from past
events -- no model at all, no feature columns read -- is worth +0.0086 against a floor at -0.0001
(round 202), and on the same windows exceeds the fitted version by +0.0059: a floor-free
comparison of two arms whose selection rules both saw sizing, not a statement about fitting in
general. Both rounds were reproduced by a checker frozen before the run.

A third result took two rounds to land, and the failure in between is worth more than the
number. Shrinking each cell toward its smoothed neighbourhood rate rather than toward the
global rate is worth +0.0029 per window over the counted map, against a floor that permutes
the cell coordinates the smoothing kernel is built from so that every cell borrows from
geographically meaningless neighbours; all five of those floor worlds came out below zero,
averaging -0.0027. The first attempt (round 204, +0.0031) passed that rule on the run and
then failed its own frozen checker on five of twenty-four checks, for two reasons, neither
of them in the arithmetic. The kernel was not uniquely determined: the contract's definition
of it gave the separation only as a great-circle distance in degrees, the one formula named
anywhere in that contract sat in a gate describing the run, and the checker reached for the
other standard one; because the arena's cells sit on a regular one-degree lattice, at five of
the six widths between six hundred and seventeen hundred cell pairs per width sit within a
billionth of a degree of the kernel's hard cutoff, and three to seven hundred of them fell on
opposite sides of it. And the checker's tolerance had been inherited from earlier rounds
rather than measured, so the check would have failed even with the formula settled. Round 206
retook it with two changes -- the formula is named, and the cutoff is removed, since the
weight four widths out is three ten-thousandths of the peak and mostly cancels between the
numerator and the denominator of a ratio -- with the prediction frozen in its own file before
the contract, the run or the checker existed, and with the tolerance measured on the
arithmetic it governs. The verdict reproduced, the size moved by 0.00016, and the checker
passed all twenty-seven checks. It is the same rows and the same floor worlds, so the two
rounds' ranks are one piece of evidence seen twice rather than two. The shape judged there
was still chosen after a different smoothing shape had failed in sizing, its width grid was
written after seeing that failure, and the size itself was measured in sizing before either
contract, so neither round is an independent estimate of it (updates 124, 126 and 127).

A fourth attempt did not land, and its contract said in advance how to read that. Earthquakes
lie along plate boundaries, so the neighbourhood a cell borrows from ought to be a line rather
than a disc. An ellipse built from the arena's own past counts and stretched along the local
axis of the cloud around each cell loses at every aspect ratio, and the control declared before
the run -- the same ellipse turned across that axis -- gains. That looked like a statement
about direction and it is not: for a cell sitting on a belt the across-belt direction is empty,
so the turned ellipse is, for the cells that carry the counts, simply a narrower circular
kernel. An isotropic kernel with no direction in it at all, whose width is set separately for
each cell by the same elongation factor, recovers almost all of it; the two differ by 0.00008
in forty-seven per cent of windows, inside its own error bar (update 128). The width, not the
direction, was then written into a contract and judged. It failed: 0.00030 per window, positive
in half of them, where the frozen rule required three fifths and a margin clear of its own
error bar. The floor there -- nine worlds that keep the exact collection of widths and destroy
only which cell gets which -- ranked the real assignment first, and none of the nine refused
the intervention, but a floor cannot turn a failure into a pass and the contract said so before
the run. The contract also said, before the run, that if the primary failed the reading was to
be that the rule cannot choose the strength, not that the width does nothing, and it named the
numbers that would have to decide that. Held at a fixed strength rather than chosen, the same
intervention is worth 0.00045 in sixty-one per cent of windows and clears all four conditions
-- reported, floorless, and therefore not a result -- and the gain lives in the twenty windows
where the earlier rule had settled on a wide kernel while it is absent or negative in the
thirteen where that rule had chosen a narrow one, the selection doing its damage in the early
windows where there was nothing to gain (update 129). That fixed strength was then promoted to
a judgement of its own, with the choice of it disclosed in the contract as having been made
after seeing it win, so that the headline number became a reproduction gate rather than news:
it gives back the stored earlier value in all thirty-three scored windows at exactly zero
difference. What was new was a second floor. The first, the same unrestricted shuffle of
widths, the arm again beat in all nine worlds -- though those nine are the previous round's own
permutations, so that rank and the previous one are a single piece of evidence seen twice
rather than two. The second holds fixed the one confound the previous contract had named and
could not resolve, shuffling the widths only within exact levels of how many cells of the arena
lie within two degrees of a cell; it reproduces that association to about one part in ten
thousand at the worst window while still moving more than ninety-nine per cent of cells.
Against that floor the arm came second of ten, so the verdict is null. The contract had given
every reachable outcome a reading in advance, and this one falls in the branch labelled
undecided: attributing the gain to the edge property required the arm to fail against that
floor's average as well, and it did not (update 130). The next step was then taken as a
measurement rather than as a third floor, by building the rival as an arm. Splitting the
elongation into the part a cell's neighbour count explains and the part it cannot, both halves
are positive and neither can be told from the other, and summed they overshoot the whole while
tracking it across windows, so they overlap rather than add. An arm that drops the elongation
from its construction entirely -- a width that is an affine decreasing function of the
neighbour count, with two scalars matched to the elongation field's mean and spread and no
per-cell seismic quantity anywhere -- is worth 0.00047 per window against the elongation arm's
0.00045, and the difference between them fails two of the four conditions. So what three rounds
pursued as narrowing the kernel where past events lie along a line is matched, within what a
sizing can resolve, by narrowing it where the arena has fewer cells nearby: a statement about
the smoother's behaviour at the edge of the studied region, not about the earth. That is a
sizing, with no contract and no floor, and its rival arms were designed after seeing the round
they rival (update 131). Measuring what an edge cell's kernel actually does then settled what
the width is and is not doing. The edge is severe: splitting the arena at the median neighbour
count, the edge half draws on 0.23 times the interior half's effective number of past events, a
cell's own weight in its own estimate rises from 0.16 to 0.37, and the largest gap in azimuth
holding no arena at all is 184 to 203 degrees against 87 to 93. The per-cell width does not
repair the rate it was supposed to repair. Against the fraction of each cell's own positives
that turned out to be large isolated mainshocks, the change in squared error is inside its own
bar for the neighbourhood rate and clearly worse for the shrunk map that enters the score and
for binomial deviance, with the damage concentrated at the edge. It does remove part of the
error that repeats across windows, and by following the elongation rather than merely by
narrowing more -- deeper than all nine of the previous round's permutations at the edge, sixth
of ten in the interior -- but it pays for that in variance, so the word correction is
withdrawn. Where the gain does live is fixed by an identity over the negatives: three quarters
of them sit in cells carrying no positive that window, where the arm already scores 0.83 and
the change fails three of the four conditions, and the remaining quarter sit in cells that do,
where the arm scores 0.58 and the change is worth 0.00127 in ninety-seven per cent of windows.
The gain is a reordering of the active cells and not a better estimate of any cell's rate. The
frozen prediction said the opposite on both counts, and the reading applied is the one written
in advance for that outcome (update 132).

Read those with their limits, which the contracts state and the log repeats. None of the three
that landed is an independent estimate of its own effect size: every effect was measured in
fitting-free sizing before its contract was written, so what a round adds is the floor and the
frozen record, not a second measurement. The base is a climatology, not the shipped forecast,
and the floors move only the counted flag -- the base is identical in every world and is never
tested. Five permutation worlds give a rank of one in six, not a p-value. The twenty floor
worlds of round 202 are round 201's own, bit for bit, so the two floor passes are not
independent evidence about each other. In round 202 the causal selection sat at the top of both
frozen grids at this threshold, so its optimum may lie outside them; no grid was widened after
seeing results. Round 206's shrinkage parameter also sat at the top of its grid in all
thirty-three windows that had a history, and a later fitting-free round measured what that
meant: taking the parameter to its limit, which removes the cell's extra weight on its own
count above its share of the kernel, is clearly worse, so the optimum lies between the top of
the grid and that limit rather than beyond it and the grid is merely coarse there (update 128).
No new observable is introduced -- the same catalogue is re-read. A within-cell shuffle of the
same labels produces a *stronger* arm than the truth, but a fitting-free follow-up (round 203)
showed why: conserving each cell's whole-record count lets the shuffle move the scored window's
own shock into the training past, so that comparison is a partial oracle and carries no
evidence in either direction. What stands is narrower -- round 202's arm is a per-cell map by
construction, since it reads no columns at all, and event-by-event discrimination has not been
demonstrated. The cost on all positives was outside these two rounds; where it was scored in
this arena (round 198) the mainshock readout cost -0.019 per window. The floor of the fourth
attempt destroys every per-cell property at once, so it cannot say that elongation specifically
is what the width tracks: the elongation measure is essentially uncorrelated with how many past
events a cell has seen, but it is correlated at about minus a quarter with how many
neighbouring cells lie within two degrees, so a cell at the edge of the studied region scores
as more elongated. A floor separating the two has since been run and did not settle it: holding
that neighbour count fixed, one of nine worlds beat the arm, which the frozen contract had
ruled in advance to be too weak either to attribute the gain to the edge property or to clear
it (update 130). Building the rival as an arm instead did settle which of the two is needed,
though only at the strength of a sizing: a width that never measures elongation at all, set
from the neighbour count alone, matches the elongation arm within what that sizing can resolve,
so the elongation is not required to obtain the gain (update 131). And magnitudes are not
compared across catalogues, so none of this is a statement about Japan. Measuring the kernel
itself then closed the question the width was raised to answer: the edge is real and severe,
but narrowing there does not improve the rate being estimated, and the gain it carries sits
entirely in ordering the cells that are active in a window against each other, where the arm
scores 0.58, rather than in any cell's estimate (update 132). That too is a sizing and carries
no floor, the nine control worlds are the previous round's own, and the neighbour count is a
property of the arena's footprint rather than a physical boundary.

Where that leaves the arm was then measured rather than assumed. Pricing the same reference
levels on that active quarter alone -- a causal climatology, a two-sided rate, a whole-record
rate and the window's own -- puts the arm below a causal climatology that counts the scored
event itself, by 0.0247 in eighty-eight per cent of windows, while on the whole problem the
deployed base rate beats that same climatology by 0.0416. The crossover is mostly about which
event is counted rather than about the map: swapping the counted event is worth 0.0449 on the
quarter and the map returns 0.0203 on top of it, so no single base rate serves both halves. A
claim did not survive the round and is withdrawn here rather than published. The two-sided rate
gains 0.0287 on that quarter, which holds a quarter of the negatives and so is worth 0.0071
pooled, but the identity runs in both directions: the same rate loses 0.0415 on the rest and
0.0240 on the whole, the roll-up reproducing that whole-problem loss to seventeen decimal
places. A component is not a lever, and the quarter cannot be singled out inside a forecaster
either, because membership of it is defined by the labels of the very window being scored
(update 133). That too is a sizing carrying no floor, and the rate that counts the scored event
was added while the machinery was being written rather than named in the frozen prediction.

Whether one score could carry both base rates was then measured, and it could not. Adding the
cell's own causal count of past large isolated shocks to the arm, at a weight chosen causally
from earlier windows only, loses 0.0004 pooled, and it beats none of the three rivals built
beside it in the same round: the cell's own rate with the shrinkage released, a single bit
recording whether the cell has ever had such a shock, and a wider weight on the smoothed map.
The duller explanation the last of those was built to test is itself refuted, the fixed-weight
curve peaking at 0.35 inside a range that runs to 0.50. A control adding the count of any shock
rather than the isolated one behaved as predicted, at precisely zero. Two readings of my own
were blocked by audit and corrected here: only two of the six families rise on the hard quarter
throughout rather than all of them, and the best trade across the five ADDITIVE families is 2.62
units gained there for each unit lost elsewhere against the 3.00 that breaking even needs, at
the smallest weight on the grids, where I had quoted 1.30 from one family alone. A third
correction, found while designing the next round and verified against the saved array: an
earlier version of this paragraph said the best trade anywhere on the grids was 2.62, which is
true only of those five additive families. The sixth family reweights the smoothed map instead
of adding to the arm, and one of its fixed points shows a positive pooled arc, +0.00038938 with
a ratio of 15.78. That is not a perturbation that beat the obstruction: its weight is the arm's
own causal choice in twenty-five of the thirty-three windows, so the arc is non-zero only in the
eight early windows where the causal path chose a smaller weight, and there it mixes gains and
losses from +0.0069 to -0.0049. It prices the causal weight choice in the early windows.
Eighty-seven per cent of
break-even at the edge of a grid is a boundary of measurement, not a wall, so the claim licensed
is only that none of the five declared additive perturbations raises pooled skill at any of
their non-zero weights. Even choosing the best weight per window with that window's own labels
the family returns 0.0018, below the 0.0029 the smoothing round already banked (update 134).
What the split exposed cannot be used directly, since membership is defined by the scored
window's labels, but a causal stand-in for it is strong: the arm's own background orders cells
by whether they will carry any shock at all at 0.785. A weight that varies with predicted
activity, and the weights below this grid's minimum, are what gets measured next.

Both were then measured, and neither pays. A weight that rises with the cell's causally
predicted activity loses 0.0004 pooled, no fixed value of it raises pooled at all, and the
curve peaks at zero weight; the own-cell rate at weights below the last grid gives pooled arcs
of +0.00003 to +0.00009, but with t statistics of +0.69 down to +0.27, no weight satisfying the
four conditions, and twenty-six of thirty-three windows carrying the same sign at all five
weights, so five looks are one look. Three readings of my own were withdrawn after audit and
recomputation: that the small weights cross break-even, that the gate's direction is right, and
that gating improves the trade. The first is a point estimate that is not resolved; the second
is a forbidden mechanism sentence that also has a duller explanation, since the gating variable
is the arm's own first term relabelled; the third compares two falling curves at different
perturbation sizes, and matched against each other the gate merely shrinks the perturbation on
both sides. The causal layer is one window: the causal weight is zero in thirty-two of
thirty-three, so the headline number is one window's arc divided by thirty-three.
What is decisively measured is the shape of the failure, and it is not the shape of a null. At
every small weight the gain on the hard quarter has a t statistic between +7.6 and +8.9 and
appears in twenty-eight to thirty-one windows, while the cost on the rest has a t between -1.6
and -2.6 and appears in twenty to twenty-three. The pooled sign is the difference between a
well-measured positive and an unmeasured negative, which is a resolution limit rather than an
absence of information; at the smallest weight it would take about two hundred windows to
decide, and this arena has thirty-three. One candidate lever was killed before being proposed:
collapsing the arm to a per-cell constant inside the window scores 0.0085 above it, but that
averages over rows later than the row being scored, and the causal version of the same idea
scores 0.00003 BELOW the arm on the same five probe windows. The next sizing therefore leaves
counts of the same events behind and prices per-cell quantities that cannot be derived from
them: the time since the cell's last large isolated shock, and the largest magnitude it has
ever produced (update 135).

Both were then priced, and the round decided a sign rather than finding a lever. Its design was
audited three times before freezing and every round found a blocking fault, all three mine: an
independence claim computed on cells that have had such a shock rather than on the vector a
family adds, where the same quantity is the count relabelled at -0.94 to -0.88; then the same
error mirrored, an all-cell correlation describing a tie block that covers 36 to 55 per cent of
cells rather than the vector, which on its own support sits at -0.46 to -0.57 with the count;
and a control whose covariance with what it was meant to rule out is zero by construction, so it
was the arm itself. The frozen text now carries a standing rule from that: never quote a
correlation computed on a population other than the one the family actually moves. The
independent check was also written before the run this time, with the absence of its output
recorded, which repairs a fault the previous two rounds shared.
Every gate passed and every prediction that would have shown a gain was refuted: the causal arm
loses 0.0000249 pooled, the largest past magnitude and the negative count weight never left the
arm so their arcs are exactly zero, and the curve peaks at zero weight. What the round decided
is that the direction declared in advance is the harmful one. A recent large isolated shock
raising the score loses at every weight, pooled t from -3.35 to -23.5, and its arc on the hard
quarter is decisively negative at t -3.72. The opposite direction is not established either: it
passes the shape bar at one weight of seven at t +0.79, and it received a real causal test,
live in thirty of thirty-three windows, returning +0.0000134 at t +0.07. So the graded age is
not a lever in either sign. Recency is not therefore anti-predictive; the licensed statement is
only about adding that term to this arm, since the arm already prices recency, the perturbation
carries an implicit bit at a negative weight while the bit at a positive weight raises the same
quarter at t +7.3, and the count component pushes the other way.
Two facts close. The negative half of the count weight, never measured before, is monotonically
harmful from -0.0196 to -0.467 with t from -8.9 to -47, which is a fact about the grid. And
across two rounds and roughly forty grid points every per-cell additive term has raised the hard
quarter decisively and paid more than that back on the rest. The arm's map is estimated from
4,634 isolated shocks at magnitude six and above while the catalogue holds 13,047 at five and a
half and 35,605 at five, so the next sizing estimates the same field from the graded magnitude
structure rather than from the sparse top of it (update 136).

That premise was false, and a five-window probe said so before anything was frozen: the map
built from isolated shocks at magnitude five is not a denser copy of the map at six, with rank
correlation -0.21 to -0.13 across cells and a cost of 0.0114 pooled when swapped in. The arm's
map factorises into the cell's isolated fraction at five and the fraction of those reaching six,
and the two describe different things. What did move was the isolated fraction counted in rows,
as the arm counts, times the fraction of ALL past magnitude-five events in the cell that reached
six, counted in events. It was chosen after fourteen variants on five windows, and eight of the
thirty-three windows were looked at before freezing. The design audit blocked my description of
it twice: the second factor is not a seventeen-times denser, different event set, because the
arm's denominator is already painted from all 78,071 driving events and 69 per cent of the new
numerator is the arm's own targets; what changed is the unit and composition of a denominator.
Rivals were built from isolated events only, from rows, from the event factor alone, and as a
residual orthogonal to the counts, and the check was written before the run.
Every gate passed and the result landed in the strongest frozen branch: the causal arm gains
+0.00182522 pooled with all four conditions, +0.00166043 with all four on the twenty-five
windows never looked at, beats the isolated-only rival by +0.00239 and the row-unit rival by
+0.00264 on both sets, and is positive on the wide side of the negatives. A post-run audit
rebuilt the scores exactly and narrowed the reading in three places, each recomputed: the causal
selection settles on the shrinkage value the probes favoured, and without it in the grid the
gain drops to +0.000831 and fails, so what holds is that value on the unseen windows (+0.002302,
t 3.92), not robustness to the grid; the gain is carried by the all-event denominator, while an
all-event numerator is not shown to matter; and the trade-off between the two sides of the
negatives has not gone away but moves along the shrinkage axis, with the chosen value at the
crossing. It is not called independent of counts, not robust to its best windows (dropping the
three largest unseen windows fails the bar), and not compared as a ratio with the smoothing
round, whose increment the arm already contains. The unseen windows are now spent for this
family, so the next step is a preregistered judgement with a floor that moves the new factor's
map between cells of matched event count and neighbourhood, at least nineteen worlds, with the
fixed value registered alongside the causal path (update 137).

That judgement was run under a floor that keeps the arm's own map real and moves only the
increment between cells of the same decile of past event count, the same third of neighbourhood
density and the same decile of the arm's level, with every world rerunning the causal selection.
Two easier floors were rejected first, because moving the magnitude factor itself destroys the
geography the arm already carries and drops every world to about 0.013 below the arm. It is not
blind: the real numbers were the previous entry's, and a design audit had already run this floor
with other seeds and found the arm above all nineteen worlds. The first run stopped at a gate
requiring ninety per cent of cells to move, a number I had guessed; a permutation inside a
stratum leaves one cell in place on average, so about 88 per cent was expected, and the assertion
sat before anything was saved or printed. The tolerance was measured without labels and the
contract re-cut with only that gate and the seeds changed. The second run passed every gate and
its check, written first, regenerated all 1,254 permutations from their seeds. The verdict is
REAL on all three: +0.00182522 against a world maximum of +0.00021937 on the primary floor
(rank one of twenty, world mean -0.00064608), rank one at the hindsight value and on the
secondary floor. A post-run audit narrowed it, recomputed here: the claim is the increment as a
whole, whose two parts track the arm's level at +0.92 to +0.98 and -0.95 to -0.99 and nearly
cancel; it is a static prior over where large isolated mainshocks occur, not a signal of when,
raising the window-mean AUC from 0.7670 to 0.7688; the real arm beats every world in only four
of thirty-three windows, so the verdict rests on the mean; and the effective sample is the fifty
to a hundred target cells per window, not the rows. Every window of this arena is now spent for
this family, so the next step carries the same increment to the Japan catalogue, where it has
never been looked at, under its own preregistration and floor (update 138).

That step was taken on 163 Japan cells chosen by the global arena rule, with an isolated
magnitude-six target and thirty-four judged windows. Only three to fifteen cells carry a target
per window, and two design audits measured the power before freezing: on twelve global
sub-arenas of that size the procedure ranked the known increment in the top five of a hundred
in five of twelve on the primary floor. The frozen verdict is not replicated at this power: the
causal increment over the arm was +0.0055 and ranked 22 of 100 on the primary floor (10 of 100
at hindsight). The secondary, residual floor ranked it 2 of 100, which the contract reports as a
number only. The map arm itself sits 0.0041 below the climatology here. The frozen independent
check failed on four of eleven items. The fault is a knife edge in the contract and the run, not
the checker: neighbourhood density counted cells within 2.0 degrees, cells two degrees apart
along a meridian sit exactly on it, and the run's radian arithmetic dropped 47 of 92 such pairs.
Using the run's values reproduces every permutation and world score exactly, which verifies what
was run but not the contract's floor. The same knife edge is in the global geometry, where 526
of 2,280 cells get a density that depends on latitude; the REAL verdict above stands as a valid
within-strata permutation, but its density was never the literal count (update 139).

Every term added since that arc began is fixed for a cell across the whole scoring year, so it can
reorder cells but never the three-day steps inside one cell-year, which is where the arm is weakest
(0.5785 against 0.8296). The next round built the first term that is mostly not cell-constant, from
an observable the project had never read: event depth, which the catalogue reader drops. It failed
its own gate. The trailing sums were differences of float32 cumulative sums; the cumulative sums
reach 600 while a 33-day window can hold 1e-6, so 32 of 200 gate draws exceeded tolerance and 37
returned exact zero where the truth lay between 1e-12 and 1e-6 -- the same knife edge as above, now
in floating point. The contract was not loosened. The re-cut takes the trailing sum as a direct sum
of eleven bins, which cancels nothing (max relative error 4.21e-16), and its independent check
passes 19 of 19. The window-constant depth map then passes the four conditions in no reading at any
grid point. The within-window column does move the arm, +0.00094050 under the causal rule with all
four conditions, raising the pooled, active and dead readings together. But a column built from the
same field with the deep flag simply deleted reaches +0.00116444 under the same rule, and
orthogonalising that direction out of the depth column leaves +0.00029472, which fails. What was
sized is a within-window activity term, not a depth term; the declared control missed it because
permuting the flag destroys the column magnitude as well as its direction. There is no floor in
that round, so none of it is a claim about the Earth (update 140).

The floor for that judgement was then built and measured before anything was pre-registered, and
the judgement was abandoned. Three floors -- a circular shift of the cell-window profile, an
exchange of profiles between cells inside strata, and a straight permutation in time -- all share
the real column's cell-window level exactly, so only the alignment differs. In eight windows of
eight, under all three, every world scored higher than the real column. The permutation floor
carries no future, so this is not an oracle: the true within-window alignment of recent activity
is anti-predictive for an isolated mainshock. Splitting the gain at the same weight, the
cell-window level is worth +0.0104 with eight of eight windows positive and the within-window part
-0.0105 with none of them; that level is not an arm, because it averages over rows later than the
one being scored, and a cell that hosts the mainshock has its level raised by the aftershocks that
follow it. Reading a slower causal level instead was refuted too: +0.0027 at 33 days falling to
+0.0012 at 366, near zero when frozen at the window start, and negative on the active-cell reading
in all ten variants (update 141).

The same question was then put to every within-window column the arena already holds: the six ETAS
variants and the cumulative count in the feature file, which the global arm never reads, plus the
trailing activity column. Each was added at both signs and priced against nine worlds that permute
it in time inside each cell-window with the level held fixed. All 128 comparisons -- eight columns,
eight windows, two signs -- agree: at a positive weight the real column ranks tenth of ten every
time, and at a negative weight first of ten every time. One reading covers that symmetry -- the
real column is a level plus a harmful structure and a world is the same level plus neutral noise.
What is closed is narrow: every within-window column this arena holds is a functional of
magnitude-weighted intensity, and the within-window ordering of intensity is anti-predictive for
this target. A shape rather than an amount -- how the trailing magnitude distribution moves, how
compact the trailing events are, how regular their inter-event times are -- is not proportional to
how much aftershock activity a cell produces and has not been measured (update 142).

It has now been measured, and it carries nothing. Three shapes were built; two failed outright and
the third -- the share of the 33-day trailing weight in the most recent 15 days -- passed a
criterion declared before the run and was, on the mainshock side, the first positive column in this
arc (+0.0039, seven of eight windows). A design audit blocked the judgement drafted on it, on four
counts that all reproduced: inside a cell-window the column's deviation correlates +0.87 to +0.93
with the trailing amount, so the floor deleted when the cell was active rather than how its
activity was split; the criterion is met by the within-window cumulative count, a monotone time
trend with no content, in eight windows of eight; the twenty-five windows held back were already
scored by the preceding round with a sibling column, which is also where the weight came from; and
a clause about the ETAS variants on the mainshock side was never measured. A repaired floor, which
permutes only within quartiles of the trailing amount, was then built -- and the content-free
columns passed that too. Measured directly, the row's index inside the window, alone, gives
+0.0019 to +0.0039 on the mainshock side, and the cumulative count +0.0089 at eight of eight; kept
positives sit at mean position 0.5189 in the window against 0.4999 for negatives. Removing the
arm's own within-window drift is worth +0.095 pooled and +0.115 on the mainshock side, but that
removal averages over later rows and is not causal, and its causal form was closed earlier at
-0.00003. What is closed is a shape, not a ceiling: the next lever has to be something a
within-window position cannot imitate (update 143).

The next entry put two numbers that had been lying beside each other onto one axis, and the first
thing it found was a leak of its own. The band of M>=4.5 events the arm already sees runs at 2.667
times its own local background in the 34 days before an isolated M>=6, measured on sixty targets;
the arm scores 0.579 on the rows of cells that hold a target in that year. Read out of one piece
of code on 2,703 targets, the elevation is 2.261 for the estimator that produced the 2.667, 2.167
with the disc moved to the cell centre, 2.006 for the Gaussian at the epicentre and 1.947 for the
Gaussian the field itself uses, and 69 per cent of sixty-target resamples fall at or below 2.667,
so the earlier figure is one draw of sixty rather than a disagreement. The alignment gate had
failed at 5.207 because the field was being read at a row whose trailing window ends up to three
days after the event, so the window said to be before the target held the target and its first
aftershocks: a factor of 3.07. A repaired read overshot the other way, the tolerance was not
loosened, and the readings that gate covered stay unread. Two measurements declared separately do
not depend on it. Normalised against the active negatives of its own cell and year, a positive row
carries 1.229 in the last three days before its target, 1.060 from three to six, and 1.01 or below
in all nine bins beyond six days. And relabelling with a target within h days, the field alone
against the frozen arm on identical rows differs by -0.0049 at three days, rising monotonically to
-0.0248 at thirty-four, with the arm not rebuilt for the shorter horizon so only the difference is
read. The label paints about eleven three-day rows before each target and the elevation lives in
one or two of them; that is arithmetic about the target definition, not a mechanism and not a
ceiling (update 144).

The hole in that comparison was closed next. Everything the arm is made of was rebuilt at each of
five horizons -- the broad label, the target paint, the causal base rate with its determined-rows
count taken at that horizon, the magnitude term's counts, and the causal choice of its three
knobs, re-derived window by window with the frozen rule -- while the scoring windows, the row set,
the trailing field and the nine-point weight grid were held fixed and declared as held. The
verifier was written and checksummed before the run. At the 34-day horizon the rebuilt grid
reproduces the frozen one to zero wherever the kernel width is non-zero, the causal selection
matches in all 33 windows, and the arm returns 0.7670, 0.5785 and 0.8296, a maximum deviation of
0.00003; the verifier passed 12 of 12, including two windows rebuilt from the arena rather than
read back. The declared primary, the causal increment of the band over the rebuilt arm on the
active subset, is +0.004649 at three days, +0.003218 at six, +0.000212 at twelve, +0.000172 at
twenty-one and -0.000635 at thirty-four: monotone as the horizon shortens, and negative at the
horizon the product uses. The declared pass required all four conditions at both three and six
days and they are not all met -- at three days 20 of 33 windows are positive, which is 0.606
against a threshold of 0.60, so the leave-one-out condition fails on that boundary -- and the
contract's remaining branch says to report the numbers and put no story on them. Reported beside
it and not read alone, because their weight was selected on another axis: on all rows and on the
rest the increment does meet all four at three and six days, and reverses sign from twelve days
on. The rebuilt arm's own levels at different horizons are different label sets with different
base rates and are not one scale. There is no floor here, so none of it is a verdict, and it does
not show that shortening the horizon repairs anything (update 145).

The floored judgement that was to follow was written, frozen and run, and then abandoned without a
verdict by a clause its own contract had put there. The floor gave each cell, window by window,
the band series of a different cell drawn from the same stratum -- deciles of the cell's mean band
level crossed with terciles of its causal past count, both of them counts, so that no threshold
sat on a lattice of cell separations. The contract said first that the round was not blind, and
that it could therefore only ask whether the real column beats worlds whose alignment had been
destroyed, never how large the effect is. Its gates passed: the identity permutation reproduces
the previous entry's increments to zero at both horizons, every world respects its strata, and the
verifier, checksummed before the run with a failing directory listing for the result recorded
beside it, passed 14 of 14. Then the clause fired. Two controls had to fail the floor before the
real column could be read, and while the one carrying the map, the marginal and the cell's own
level but no alignment did fail, ranking 73rd and 60th of 100, the one carrying only the per-cell
map ranked first of 100 at both horizons, +0.029018 against worlds at +0.019408. So the floor does
not isolate what it claims, and the real column's rank was neither printed nor read. The reason
was then measured. The strata number thirty, a world's map keeps a rank correlation of 0.990 with
the true one -- and yet 54 to 83 per cent of the spread in the cell's mean band level survives
inside a stratum, with 32 to 63 per cent surviving even at fifty quantile bins. The quantity is
heavy-tailed, so strata preserve the ordering almost perfectly while leaving most of the level
inside the bin, and only the true assignment is exactly right; a rank correlation would have
hidden that. Neither control number is a lever, because a cell mean averages over rows later in
the window and so carries the target's own aftershocks. The next cut rebuilds the floor from the
control that behaved, and must declare in advance that two content-free columns have to fail it
(update 146).

The replacement floor was broken too, and by the same two empty columns as before. It permutes,
inside each cell and each window, when the band took each of its values, so a cell keeps its mean,
its maximum and its whole marginal distribution and loses only the order -- the construction that
had behaved correctly as a control in the abandoned round. Before the run its contract was re-cut
once, recorded rather than hidden: the first version required a map-only column to be bit
identical across worlds, which is a knife edge, because permuting a cell's values changes the
order a floating-point sum is taken in. The tolerance was not loosened; the check moved onto the
multiset, which is exactly invariant, and the mean's deviation became something reported, measured
at 1.489e-15. The contract also declared in advance that this floor is conservative: a world can
carry a value from after a target onto a row before it, which can make a real effect fail but
cannot manufacture a pass. The gates passed -- the identity permutation reproduces the previous
increments to zero at both horizons, every world keeps its cell's multiset exactly, 0.82 per cent
of rows stay in place, and the verifier, checksummed before the run, passed 13 of 13. Then the
clause fired: the within-window cumulative count ranked first of 100 at +0.011626 against worlds
at +0.004323, and the row's index inside the window ranked third, so both controls that had to
fail passed, and the real column's rank was neither printed nor read. This is the fourth time a
floor permuting time inside the scoring window has been broken by a column with no content in it,
three times at 34 days and now at three, which closes that family of floors for this arena -- a
way of building a floor, not the question it was built to ask. The reason was then measured: kept
positives sit slightly later inside their window than negatives at every horizon, 0.5056 against
0.5000 at three days, which is a route by which any column smooth in the window's own clock beats
a scrambled copy of itself while carrying nothing. It is not claimed as the whole explanation,
since the bias is positive in only 18 to 20 windows of 33 and the row index alone gains 0.0012
where the cumulative count gains 0.0073 (update 147).

The third floor was abandoned too, and this time the fault was located. In it each cell reads its
own real band series from another part of the record, cyclically shifted, so a world keeps the
positional structure, the smoothness and a real band's distribution and loses only its alignment.
The groundwork measured first that such a floor could not be built naively -- the band's level
runs across the scored era by a factor of 2.68 against a limit of two declared in advance -- so
the era was split where each half satisfies that rule, 1.33 and 1.78, and a window is shifted only
inside its own half, never by less than two years, at which lag the band's autocorrelation is
0.035. The guard was deliberately not widened, since any correlation a world keeps raises its
score and makes the floor harder. Every gate passed: the zero shift reproduces the earlier
increments exactly at both horizons, every world's rows are a contiguous cyclic block inside its
half past the guard, and the verifier, checksummed before the run, passed 16 of 16. Then both
controls ranked first of 100 -- the within-window cumulative count at +0.011626 against worlds at
+0.003623, the per-cell mean at +0.029018 against +0.000679 -- so the round is abandoned and the
real column's rank was neither printed nor read. Measured afterwards, each of those columns
predicts on its own: the per-cell mean alone reaches 0.6007 on the active subset in all 33 windows
and the cumulative count 0.5331 in 31, and the per-cell mean is mostly self-knowledge, since
making it causal drops its increment from +0.029018 to +0.006913. A floor that changes the year
destroys contemporaneity and both controls carried it, so both had to pass: a column is content-
free only with respect to a particular floor, and these two were carried over from one that
destroys order alone. That withdraws a word from the previous entry. The cumulative count has been
called a column with no content in it since the twenty-sixth round of this arc, and it is not one;
the content-free column is the row's own index inside the window, which broke that floor at rank 3
of 100 by itself, so the previous abandonment stands and only the wording changes. Three abandoned
floors say how floors and controls were built, not whether the band holds anything (update 148).
Before designing another floor the arc asked which dimension it was working in, and the answer
changed what was built. The arm is constant inside a scored window, and on the mainshock subset
only 0.15 per cent of the (positive, negative) pairs sit in the same cell, so its ordering is
between cells. Ordering cells by their in-window positive-to-negative ratio is optimal among
window-constant scores and reaches 0.9482 at 34 days; the corresponding oracle over scores that
are the same in every cell and vary only with the row reaches 0.6043. Both read the labels. A
third family can be bounded without an oracle: for a static score, one number per cell held fixed
in every window, the mean over windows of the subset AUC depends only on the cell ordering and is
a sum over pairs of a matrix M that sums to one, so no pair contributes more than the larger of
its two entries and the mean is at most 0.8112 at 34 days and 0.8047 at three -- for every static
score, including one fitted on the scored labels, recomputed two independent ways to 1.1e-16. Maps
re-estimated at each window start, scores that re-rank cells using the window's own active set,
and scores that vary by row are outside that bound and are named rather than left implicit. On 25
scored windows the ladder runs: the arm 0.58288; the arm flattened to its per-cell mean 0.59033,
higher, so the arm pays 0.0075 for row-level jitter; a leave-one-out per-cell ratio 0.59368; its
positive count alone 0.59943; those counts smoothed over half a degree 0.60344; the oracle
0.95017. Posed directly as a prediction problem -- for each cell active in a window, will it hold
an isolated M6 this window -- with thirty features from catalogue events before that window's
training cut and a model trained only on windows whose scoring had finished by that cut, it
reaches 0.62086, +0.03799 over the arm, higher in 23 of 25 windows, Wilcoxon p 2.1e-6, and +0.0281
after discarding the best five. The increment splits exactly three ways: +0.00745 row-level
jitter, +0.01311 being a better static map, +0.01742 the estimate being recent, the last isolated
by retraining on features from older cuts, which stays causal and can only lose information --
0.61285 at five windows of staleness, 0.61030 at ten, 0.60304 frozen at one early cut, level with
the best static map, and 0.60445 frozen in coefficients too, below the proved bound as it must be.
An audit returned three blockers and five serious findings, all reproduced from the data before
being accepted and all mine. "Everything above 0.5892 must change between windows" was false,
since the positive count alone reaches 0.59684 under the same rule and smoothing 0.59970, so that
family is not used up and its proved ceiling leaves a fifth of the scale unclaimed. A line printed
as "the same windows" compared 25 windows against a 33-window mark. The claim that a
previous-window indicator fell below the static map was the same mismatch: matched, the difference
is -6e-7, indistinguishable from zero, though the ties it could reorder carry 5.96 per cent of the
pair mass, so the test had power. The time-since-last-event features looked anti-predictive at
0.4267, but 129 to 201 of the eligible cells had never had such an event and sat pinned at the
sentinel: never-having-fired scores 0.4435 alone and the recency among cells with a history is
0.4748 and near null, the count channel with its sign flipped. Calling the arm 98 per cent of the
static map was wrong twice, since an AUC ratio is read from a half and that map reads later
windows -- the past-only version is 0.5753 where the arm is 0.5809, so a causal static map does
not beat the arm. One gate failed on its own terms, at 1.07e-03 against a declared 1e-12 because a
feature store was kept in single precision, and was repaired by changing the store rather than the
tolerance. Two other gates are recorded as constraining nothing, and a local search weak enough to
reach only 0.514 from random starts found 0.7594, which is the best ordering found and not the
static optimum. There is no floor: this is a sizing, nothing in it is real, and all 25 windows
have now been seen. What is established is where the room is -- no score fixing one number per
cell reaches 0.90 here however fitted, the part that changes between windows is real and worth
+0.017 so far, and the honestly estimable static map sits at 0.6034 against 0.8112 because 510 of
the 1,141 cells that ever fire do so in exactly one of the 33 windows (update 149).
The room the proof left open was then attacked with fields that do not count the cell's own
earthquakes: trench geometry from the USGS Slab2 repository (public domain, CC0), the plate
boundary steps of Bird (2003) whose supplement carries no licence statement, and the geodetic
strain rate of Kreemer, Blewitt and Klein (2014) under CC-BY-NC-SA 3.0, the last two used only as
local inputs with nothing derived from them committed or published. The independence is of the
pipeline, not of the sources -- Bird drew the traces partly along seismicity, the trench file is a
modification of Bird, and the GSRM deforming zone is delineated partly from seismicity -- so what
is true is only that none of the twenty-seven fields reads the count of events in the cell being
scored. They carry real information about which cell: against a within-arena permutation floor,
each field shuffled across the 2,280 cells three hundred times with a null spread of 0.0077, nine
of the twenty-seven lie outside a Bonferroni band, the strongest being the Gaussian-weighted
subduction velocity within nine hundred kilometres at 0.5617, z +8.05, while distance to the
nearest trench is informative in the direction physics predicts for a distance at 0.4634, z -4.50,
which is not an inversion; all five strain rate fields are inside the noise band. The count of
windows above a half is not evidence and is reported only beside the z, since a static field gives
the same ordering in all twenty-five windows and the eligible sets overlap heavily. They are
nevertheless decisively worse than the catalogue as a map of which cell: the tectonic model alone
reaches 0.55642 against 0.60445 for the causal static map, a paired difference of -0.04803 with
standard error 0.00833, higher in three of twenty-five windows, Wilcoxon p 1.2e-5, and the
trench-only variant reaches 0.52746. On top of the catalogue they add nothing measurable, and the
interval is the result rather than the sign: adding all twenty-seven to the frozen catalogue model
moves the mean by -0.0027 with standard error 0.0028, higher in twelve of twenty-five, p 0.40; as
genuine static maps -0.0030, higher in ten; at each window's own cut -0.0041, higher in eleven.
Three of those four are indistinguishable from zero, so the honest statement is that the data
exclude a gain larger than about +0.003 against a room of 0.21. Both clauses of the pre-registered
reading fail, the drop-the-best-five margins being -0.0075 and -0.0085. The null is about the
fields and not the learner: an out-of-cell five-fold fit allowed to read every scored label
reaches 0.54447, below the causal figure. Of the two encumbered sources it is the unlicensed one
that carries what little there is -- removing the strain rate changes the result by -0.00035, p
0.71, while removing the Bird fields costs 0.02930, t +4.63. The reason was measured rather than
told: against six thousand random cells outside the arena in the same latitude band, distance to
the nearest plate boundary separates arena from non-arena at 0.904 while inside the arena it is
0.5085 and inside the noise band, so the seeding rule of at least two M5 events has largely spent
tectonics already; that is consistent with range restriction and is not a proof, which would need
the same target on a cell set not selected on seismicity. Four defects of my own are recorded
rather than repaired in silence: the first run's verdict line implemented a rule the contract does
not contain and never computed the comparison the contract declared; the mark it used was labelled
the best static map the catalogue can honestly supply when it reads the other twenty-four windows
including later ones and is neither static nor causal; a gate declared as checking that no
tectonic field varies with the window printed three counts and asserted nothing; and each plate
boundary midpoint was an arithmetic mean of longitudes, so six of the 5,819 steps that straddle
the antimeridian were placed near zero degrees east, two of them subduction steps at 58 and 72
millimetres a year, which moved the strongest field for 242 of the 2,280 cells and changed its
score by +0.0002, and none of the six existing gates could have caught it, so a new one requires
each midpoint to lie on its own step. There is no floor here, nothing is called real, and all
twenty-five windows had been seen before the round was designed (update 150).

Whether the room that proof leaves open is learnable at all was then measured directly: the
thirty-three scored windows were split in two, a static ordering of the cells fitted on one half
and scored on the other, over seven splits -- odd against even, early against late, five random
halves -- in both directions, fourteen fits, under four rules (the ratio of firing windows to
eligible windows, the positive count, that count smoothed with a half-degree kernel, and a local
search on the training half's own score). Held out the rules reach 0.5755, 0.5840, 0.5883 and
0.5726, individual fits spread from 0.5579 to 0.5999, against 0.769 to 0.810 in sample. My first
reading of that gap -- that the room is not learnable and the static family is closed -- was
wrong, and an audit returned three blockers and five serious findings against it, every one
recomputed from the primary counts before being accepted. What is broken is coverage, not
fitting. Decomposed by how often a cell fires, with the parts recombining to the whole exactly,
the 510 cells that fire in exactly one window carry 0.1935 of the positive mass and fall from
0.6052 to 0.1505 when their own window is withheld, the 263 that fire twice fall from 0.6965 to
0.5177, and the 368 that fire three times or more, carrying 0.6184 of the mass, hold 0.8262
against 0.7482: fifty-two per cent of the drop sits in the cells that fire once and most of the
mass transfers. The half-split says the same from the other side -- 0.6638 of the held-out
positive mass lies in cells that also fired in the training half, where the smoothed map scores
0.7361 against its own hindsight 0.7591, while the remaining 0.3362 scores 0.2965, below chance,
because a map fitted on labels must bury a cell with no precedent at the bottom. Nor is the
static family at the arm's level: held out one window at a time on the same thirty-three-window
frame the ratio map reaches 0.5892 against the arm's 0.5785, the count map 0.5968 and the
smoothed map 0.5997, higher in twenty, twenty-nine and twenty-eight windows at t +2.23, +5.21 and
+3.94, while the activity-only map, which reads no label, is 0.5557 and below the arm in thirty
of thirty-three, so the labels teach about +0.044 over activity. Nor can learnability be denied
from these numbers: fitting on k windows and scoring on the rest, the smoothed map goes 0.5542,
0.5596, 0.5729, 0.5834 and 0.5981 for k of two, four, eight, sixteen and twenty-four, still
rising over the range the test side can measure -- the audit read k of thirty-two as rising too,
but my recomputation puts it at 0.5821 on a single test window, so that is not claimed -- and a
causal expanding-window static map over twenty-eight windows reaches 0.5855 against the arm's
0.5809. Five further findings are recorded rather than repaired in silence: the declared pass
line of 0.65 to reopen and 0.62 to close could only return one of its two answers, since 0.5892
on thirty-two windows was already published; calling the local search textbook overfitting was an
overstatement, its held-out cost being 0.00287 in all fourteen fits against an in-sample gain of
0.00042; twelve of the fourteen fits train on later windows and are cross-validated rather than
causal; the contract said each half's own ceiling would be computed and it was not, and measured
now those ceilings are 0.8445 to 0.8604 against 0.8112 for the full record, so eighty-five per
cent of the in-sample excess is the ceiling itself rising and attainment is 94.8 against 93.6 per
cent; and the early-against-late split is best held out only because the later windows are easier
on every yardstick, the arm going 0.5700 to 0.5866 and the per-window cell oracle 0.9433 to
0.9527, with 795 cells firing early, 789 late and only 443 in both. Geology does not fill the
hole either, which was measured rather than assumed: on cells with no precedent the fields that
were strongest across the arena are all at or below chance, 0.4657, 0.4524, 0.4838 and 0.4527,
and a hybrid that orders precedented cells by the label map and the rest by the strongest field,
reading no test label, reaches 0.5878 against 0.5883, a difference of -0.00046 with standard
error 0.00096. What the label map drops is quiet in the catalogue and unremarkable in the
tectonic fields alike. There is no floor here, nothing is called real, and all thirty-three
windows had been seen before the round was designed; the narrow statement that survives is that
thirty-three windows of counts do not bring the room below 0.8112 out, that the loss is
concentrated in cells holding a single positive which neither the counts nor the twenty-seven
static fields reach, and that the static family is nevertheless above the arm by +0.0212 and
improving as the record lengthens (update 151).

What that coverage is worth was then measured exactly, on the sudden axis itself. Every
eligible cell of every scored window was put in one of four classes by the number of
isolated M6 events it holds strictly before that window's feature cut -- none, one, two,
three or more -- and the window ACTIVE AUC was decomposed over the sixteen blocks formed by
the class of the positive cell and the class of the negative cell, which is exact because
that AUC is a sum over cell pairs weighted by the two cells' row counts; the blocks sum back
at 1.11e-16 and the arm reproduces its published 0.5785 at 3e-5. Fifteen per cent of the
ACTIVE positive mass sits in cells with no precedent, a share that falls from 0.2665 in the
first scored window to 0.1016 in the last. The blocks where the positive cell has the higher
class carry 0.4432 of the pair mass at 0.8924, spread from 0.6983 to 0.9797; the four
diagonal blocks carry 0.2983 at 0.5859; the blocks where the positive cell has the lower
class carry 0.2585 at 0.1956. The four blocks whose positives have no precedent carry 0.1532
at 0.2537, and the arithmetic ceilings -- 0.65860 if that mass scored a half, 0.73521 if it
were perfect -- are oracle bounds that flattening cannot reach, which was measured rather
than asserted: a constant added to every no-precedent cell lifts that mass to 0.4747 and
drops the total to 0.60931, one constant for the whole class gives 0.59210, and removing the
class separation entirely gives 0.57435. The class ordering is not the mistake either --
observed firing rates by class are 0.0781, 0.1189, 0.1118, 0.1688 against mean predicted
0.0771, 0.1109, 0.1295, 0.1701, and shrinking the between-class component of the predictor's
logit loses, 0.57435 at zero against 0.62086 as fitted. What is left is the ordering of cells
inside a class, and that lever now has a bound. Keeping the predictor's score values class by
class, so each class keeps its exact multiset and therefore its level and its overlaps, and
re-assigning them inside each class by the window's true positive-to-negative ratio reaches
0.86882; a better key in the same family reaches 0.87568 and a swap polish 0.87581, so the
ratio order is a lower bound and not the ceiling I first called it; and maximising all
sixteen blocks separately, a relaxation and therefore valid, bounds the whole family at
0.87777. That is the number that matters, because it is below the goal: with the predictor's
class placement held fixed, no rearrangement of cells inside their classes reaches 0.90, and
at least 0.0222 has to come from a better placement of the classes themselves. The
unconstrained per-window cell oracle is 0.95017, so the room exists, but not inside this
family. The within-class gain of +0.24796 also does not land where I said: 42.8 per cent of
it appears in the diagonal blocks, 38.5 per cent in the blocks whose positives have the lower
class and 18.7 per cent in the upper blocks, because a correctly ordered class puts its
firing cells at the top of its own range where they overtake the negatives of higher classes.
Stacking the classes strictly with the same oracle inside each reaches only 0.72425, which is
a particular construction and not a ceiling, and it is low mechanically, since a strict stack
sets the whole inverted mass to exactly zero. The requirement is therefore a number: blending
the within-class ordering from the predictor towards the oracle, a total of 0.70 needs a
within-class AUC of 0.6925, 0.75 needs 0.7669, 0.80 needs 0.8427 and 0.85 needs 0.9152,
against 0.5859 today. Something does order cells inside a class, against a permutation floor
that preserves every class level and destroys only the within-class ordering: the predictor
reaches 0.5859 in the diagonal blocks where the null sits at 0.4993, spread 0.0071. But two
of the strongest single fields are not fields -- the isolated-M6 count is the class variable
uncapped, rank correlation 1.000 inside the top class, and the weighted count 0.968 -- so the
half-degree smoothing at 0.5552 is the one of the four that also varies inside the lower
classes, one tectonic field (subduction velocity within nine hundred kilometres) reaches
0.5297, and in the top class the predictor's 0.6031 against the raw count's 0.5713 is the
only non-trivial diagonal number. Where neither cell has a precedent, two-degree smoothing
reaches 0.5981 -- not a sentinel, since the field is never zero there, median 2.3510, tied
pair mass 0.0056, above a half in 24 of 25 windows -- which is z +5.69 against a conservative
null that shares one cell relabelling across all windows, rather than the +6.69 an
independent-window null gives. So within-class ordering exists at the size of hundredths
while the requirement is tenths. A dozen defects are recorded rather than repaired in
silence: a null built at the wrong level, so the run's closing decision line and its stored
null spread are retracted; "about 0.83" for the remaining requirement, which mis-stated the
upper range and assumed the upper blocks rise to one, when with them as measured it is
0.9061; the claim that a within-class re-assignment leaves the cross-class structure
untouched, which the three-way split above refutes; the claim that the ratio order is the
exact ceiling, wrong by 0.0070; the claim that the class placement is not the bottleneck,
refuted by the 0.87777 bound; a strict stack called a ceiling; six declared gates that cannot
fail by construction, including a block identity that holds for a random partition the audit
fed in, and a further gate that only checks transcription; a multiplicity correction that
standardised each draw by a spread including that draw, moving the cuts from 3.02, 3.18 and
3.24 to 3.31, 3.53 and 3.60 and dropping two passers; sixty draws carrying about nine per
cent of error on every spread quoted; two scanned candidates that are identically zero in the
class with no precedent; a share of zero in sixty draws that is a bound of one in sixty-one;
a z of +5.78 that is +0.0045 of block AUC because its null spread is 0.0008; and a training
window rule that omits the thirty-four day horizon, whose realised margin is nevertheless at
least 299 days over all 475 selected pairs. There is no floor anywhere here, nothing is
called real, all twenty-five windows had been seen, and the ceilings read the scored labels
by construction (update 152).

Two sizing rounds were then run against that within-class number. The first built the moment
budget -- what the geodetic field has loaded a cell against what the cell has already
released -- which is the one physical quantity separating two cells with the same earthquake
history that this project had never assembled. Released moment is the base ten logarithm of
the summed 10^(1.5 m + 9.1) over driving events strictly before each cut; loading is the GSRM
v2.1 second invariant of the strain rate and its smoothings, a CC-BY-NC-SA 3.0 source used
only as a local input with nothing derived from it published. Without per-cell elastic
constants or seismogenic thickness the budget can only be a difference of standardised
logarithms, and it is empty: inside the classes the three differences reach 0.4946, 0.4972
and 0.4987 against a null of 0.5000 with spread 0.0070, the overdue term and the released
moment reach 0.5158, and nothing passes the multiplicity cut of 2.48, while the predictor
reaches 0.5859 and the smoothed isolated-M6 count 0.5552 on the same pairs. They are not
relabellings of the class variable, correlating with it at -0.55 to -0.43; they are simply
flat. An audit then found almost everything around that null weaker than I had written, and
each finding was recomputed from primary data before being accepted. The gate advertised as
the repair of the previous round's unfailable gate is itself unfailable: it asked whether the
released moment changes when the cut moves a year either way, but every window holds 1,062 to
2,376 driving events in a year, so it changes for any cut, and a released moment deliberately
leaked thirty or a hundred and eighty days past the cut passes it 25 of 25 in both
directions. That is an unfailable gate written twice in succession, and it leaves this round
with no test of the causality it claims to gate. The quantity printed as a Spearman
correlation uses ordinal ranks against a class variable with tie blocks of thousands of rows,
against this project's standing midrank rule; recomputed properly the released moment sits at
+0.8237 rather than +0.7967, crossing the 0.8 line the declared reading uses. The release rate
is not a separate candidate -- the elapsed years are one scalar per window, so its ordering is
identical to the released moment's to fifteen decimals, and the rate the design described
never enters any candidate. The overdue term's 0.3981 where neither cell has a precedent is a
sentinel: no cell there has ever had an isolated M6, the time-since term is constant to the
last bit, and the number is the strain-rate ordering already measured null. The equal
weighting sits on a cancellation point, the weight sweep running 0.5082 to 0.4842, though the
whole range stays inside the declared band. Three hundred draws do not resolve the test: with
three thousand the spread is 0.00715 and the cut 2.414, and across ten independent
three-hundred-draw nulls the overdue term's z runs 2.10 to 2.39 against cuts of 2.30 to 2.76,
so it flips in one of them; the design also promised a leave-one-out standardisation the code
does not contain. The standardisation is taken over a cell set selected using the scored
labels, so "all causal" overstates it, and the printed conclusion that the next observable
must come from outside the catalogue is contradicted by the same table, whose best single
within-class field is a catalogue quantity. The null is about this construction -- no area,
no seismogenic thickness, a one-degree cell mean as the loading proxy -- and not about moment
budgets in general. The second round bounded what a map can do inside the classes at all.
Restricting the pair-mass argument that proved the 0.8112 static ceiling to pairs of cells in
the same class, and renormalising by that restricted mass of 0.2983, the best any single
fixed number per cell can do inside the classes is 0.81106, against 0.94203 for a score
re-estimated every window and 0.58592 for the predictor today; the same code without the
restriction reproduces 0.811209, and a random static ordering gives 0.5055 with its negation
at 0.4945, summing to one exactly. Read along the blend of the previous round, 0.81106
corresponds to a total of about 0.779 and 0.94203 to 0.86882, so the distance to the goal
separates into three measured pieces: a perfect static map inside the classes reaches about
0.78, adding perfect window-to-window resolution inside the classes reaches 0.869, and the
last 0.031 must come from placing the classes better, which is binding because the whole
rearrangement family is bounded at 0.87777. The per-window cell oracle is 0.95017, so the
room exists. A second defect of mine: that round's pass line was declared at 0.80 without
noticing the requirement it was meant to test is 0.8427, so the branch fired mechanically and
printed that the bound is at or above the requirement when it is below it; the condition, the
numbers and the gates stand, the sentence does not. What follows is narrow: inside a
precedent class there is still 0.225 of map between the predictor and what any static map
could do, before any of the distance becomes a question about which window, so the next
observable is a static field separating cells with identical earthquake histories, and three
candidates were checked for reachability rather than assumed -- the GEM Global Active Faults
database under CC-BY-SA-4.0, the Slab2 depth and dip grids whose licence file states United
States public domain, and the Global CMT catalogue, which answers a keyless ranged request.
No floor beyond a within-class permutation, nothing called real, the bounds read the scored
labels by construction, and the next round will carry a gate that can actually fail
(update 153).

The faults themselves were then tried, since two cells with the same earthquake count are not
alike if one holds a long fast fault and the other does not. The GEM Global Active Faults
database (CC-BY-SA-4.0) was taken as a local input only -- neither it nor any derived table
is committed or published -- and eleven static per-cell fields were built from its 13,696
traces by sampling every five kilometres onto the one-degree cells: length in the cell and
smoothed, a fault moment rate as length times width times slip rate and its smoothing, the
largest slip rate within 100 km, distance to the nearest trace and to the nearest fast one,
and the share of nearby length by slip type. Coverage was measured first, because coverage
ended the geodetic rounds: 1,545 of 2,280 cells contain a trace, 1,856 have one within
100 km, 1,468 have a slip rate within 100 km, median distance 37 km. Two build gates failed
and were repaired rather than loosened -- an earth radius of 6371.0088 against the 6371.0 the
rest of the code uses, caught at 1.08e-04 km, and thirty records carrying a literal nan in
the seismogenic depths, which float() accepts. Then the first measurement passed two fields
and an audit found why it should not be believed: the neighbourhood kernels were written as
exp(-0.5*(d/300)^2) with d from the arena's distance matrix, whose docstring says haversine
DEGREES -- measured, 111.195 km per unit, maximum 180.000 -- so every "300 km" kernel was a
300-degree one reaching all 2,280 cells instead of 50, five of the ten fields were global
quantities with a faint gradient (the smoothed length spanned 196,482 to 203,569 km where the
corrected field spans 0 to 6,549), and the floor I had rebuilt to be "spatially matched" was
hemispheric as well, which is why its spread looked 70 per cent wider than white noise. Two
earlier findings had been withdrawn on the strength of that floor, and both withdrawals are
retracted: rebuilt with true kilometre kernels at 55, 110, 220, 300 and 900 km, four thousand
draws and cuts from disjoint blocks, the subduction-velocity field survives inside the classes
under every floor but one marginal case, and where neither cell has a precedent the predictor
at 0.5808, the half-degree smoothed count at 0.5843 and the one-degree smoothing at 0.5883 all
survive too. The general claim drawn from the bad floor was also too strong: at realistic
scales a matched floor is barely wider than white noise (0.00730, 0.00742, 0.00796, 0.00847,
0.00835, 0.00851 from white through 900 km), so the lesson is to gate a kernel on how many
cells it reaches, not that white noise is generally too narrow; matching matters in the small
no-precedent block, where the spread runs 0.0166 to 0.0218 at 300 km and 0.0308 at 900 km.
The preparation was rebuilt in kilometres with gates that can fail: the cell assignment is now
checked against an independent nearest-centre search rather than against the floor key that
produced it -- an identity that passes on random points with no fault data at all -- the
shuffle check now moves every gridded field rather than the four it happened to touch, and a
new gate requires a kernel of stated radius to reach a plausible number of cells, reading 50.1
per row now against 2,280 before. Against a floor built from the same 300 km kernel in the
same units, with an eleven-candidate cut of 0.0231, every fault field lies inside the cut in
the diagonal blocks, the largest being the reverse share at 0.5174, so the pre-registered null
reading fires: this database, at one degree, against this target, does not order cells inside a
precedent class. The negative control passed at 0.5034. But where it matters most the table
says something else: in the block where neither cell has a precedent, cut 0.0587, six of the
eleven clear it -- the normal share 0.5842, smoothed length 0.5766, reverse share 0.5731,
distance to a fast fault 0.5712, and the unclassified share and moment rate the other way at
0.4100 and 0.4275 -- and so do plate-boundary age 0.6146, trench density 0.6028 and the
two-degree smoothed isolated-M6 count 0.5981, all surviving every kilometre-scaled floor. That
is consistent with the earlier round that found geology at or below chance for cells without
precedent, because it compared those cells' positives against every negative in the arena,
where any score correlated with the class must lose. Caveats: a lower seismogenic depth parses
in 3,291 of 13,696 features and a dip in 5,484, so the median feature uses both defaults and my
description of the database overstated what it carries; five of the first version's gates could
not fail by construction; three of the corrected run's gates print rather than assert. This is
sizing -- no floor beyond the within-class permutation, nothing called real, all twenty-five
windows seen. What it leaves points somewhere specific: faults do not separate cells that
already have a history, but in the cells that have none, the fifteen per cent of positive mass
a label map must bury, static geology and the widest catalogue smoothing both carry ordering
that survives a floor matched to their own smoothness (update 154).

A predictor was then built for that population alone: for each scored window, the cells with
no isolated M6 before its feature cut, fitted on the same population in earlier windows only,
with the seven static fields and a two-degree smoothed isolated-M6 count. Every gate is
asserted rather than printed, including a leak check that can fail -- a deliberately leaked
fit, the scored window added to its own training set, must beat the causal one, and does,
0.6326 against 0.6154. In the block the causal predictor reaches 0.61538 against 0.58084 for
the general predictor, and two floors declared beforehand both put it first of twenty-one:
labels shuffled inside each training window's no-precedent population, worlds 0.4498 to
0.5939, and the seven static fields shuffled across cells, worlds 0.5777 to 0.6009. Splicing
it into the whole axis moves the twenty-five-window ACTIVE AUC from 0.62086 to 0.62257,
+0.00171. An audit then narrowed almost all of that, and its four consequential findings were
recomputed from primary data before being accepted. The missing control: refitting the general
predictor's own thirty catalogue features on the no-precedent population alone, with no new
data, already reaches 0.60026, so of the +0.0345 over the general predictor, +0.0194 is
population specialisation and +0.0151 is what the static fields add. The fields do not
combine, which is what the round set out to test: dropping each in turn, five of seven make
the model better by their absence (0.6215 without trench density, 0.6193 without fast-fault
distance, 0.6188 without subduction velocity, 0.6172 without smoothed length, 0.6161 without
the normal share), and the eight-feature model beats plate-boundary age fitted alone by
0.0008, 0.6154 against 0.6146. The smoothed count alone is 0.5981, so the geological
increment is about +0.017 and nearly all of it is that one field -- itself selected on these
same twenty-five windows in the previous round, a selection neither floor prices, so the
nominal one in twenty-one is not an operating error rate. The whole-axis number is not an
ordering gain either: the ordering alone, the new ranking applied to the general predictor's
own values, is worth +0.00380 while the levels alone are worth -0.00256, because the new
model is miscalibrated here -- against an observed rate of 0.0769 the general predictor sits
at 0.0761 and the new model at 0.0874. And the training rule advertised as stricter is not:
requiring a training window to finish its scoring a horizon before the cut excludes nothing
the old rule did not, in any of the twenty-five windows, and the assertion enforcing it takes
its minimum over the set that condition defines, so it cannot fail. Three further limits: the
scored population is itself chosen using information from inside the window, since a cell is
eligible when it has a driven row there -- shared with the general predictor, so the
comparison is fair, but "causal" applies to the model's inputs and not to the set it is
scored on; the static tables are present-day compilations assembled from a record that
includes the scored windows, a leak channel no gate here can see; and a one-in-twenty-one
rank supports a one-sided p of 0.0476 at best. What survives: the ordering inside the
no-precedent population is real against both floors, most of the gain is the unglamorous fact
that fitting on that population alone beats fitting on everything, the geological part is one
field selected on the same windows, and on the whole axis the ordering is worth +0.0038 with
the model's own miscalibration giving back two thirds of it. None of it is in the operating
forecast (update 155).

The calibration that ordering gain needed was then fitted, causally: the prediction for each
training window comes from a model fitted only on windows earlier than it, and every label was
complete at least 299 days before the scored window's feature cut. It recovers almost nothing.
Isotonic calibration moves the twenty-five-window ACTIVE AUC from 0.622572 to 0.622675, or
+0.000103, and Platt to 0.623334, or +0.000760, against a line of 0.6240 declared before the run.
An audit then refuted the reasoning behind that line. The 0.624663 called here the ceiling any
monotone calibration can reach is not one: the construction fixes the multiset of values inside
the block to the general predictor's own, while a monotone map may choose any non-decreasing
values and so interleave that block differently with the cells it does not touch. It is a lower
bound, and the proof was printed eleven lines above it in the same log -- a deliberately leaked
isotonic calibration, which inverts no pair and is therefore monotone, reaches 0.626522, and one
constant added to the raw scores reaches 0.624800. So the level cost is a calibration problem
after all; what is true is that the two causal calibrators recover +0.0001 and +0.0008 of a gap
worth at least +0.0040. One candidate cause is measured: the calibration rows come from earlier
windows, where this population fires at 0.0977 against 0.0769 in the scored ones. Three of the
five gates cannot fail -- the monotonicity gate passes when fed a constant map whose block AUC is
exactly 0.5, the leak gate passes on a leak turned into a no-op, and the causality gate compares
two quantities computed from the same array by the same formula and never touches the calibration
path at all. The properties they were meant to guarantee do hold, checked directly, but by
construction rather than by test. The selected field does survive its own split: plate-boundary
age reaches 0.5665 in the no-precedent block on the eight early windows, cell-shuffle p 0.0050
over two hundred worlds; pricing the choice of field among seven gives p 0.0398 in one draw and
0.0149 in another, which is itself the finding that two hundred worlds do not settle this, and
the shortlist survived twenty-seven and eleven candidates that nothing prices. The early windows
are also a different population -- 576 eligible cells against 655, 0.2235 of the positive mass
against 0.1532 -- and all seven fields are lower there, by 0.0399 on average, which says the
population changed rather than that this field replicated. Finally, refitting the general
predictor's own thirty features on that population raises the block from 0.58084 to 0.60026 while
the whole axis falls by 0.00019, and an exact sixteen-block decomposition settles why. A splice
moves only the seven blocks touching class 0, whose masses are 0.04171, 0.11150 where those cells
are the positives and 0.22298 where they are the negatives. The refit is +0.000359, +0.002963 and
-0.003517 across them; the static model is +0.001451, +0.009028 and -0.008668. The whole-axis gain
of any splice is therefore essentially its own small block, because the cross-class movement it
causes very nearly cancels, and the cancellation is structurally against it: class-0 cells carry
exactly twice as much negative pair mass as positive, so a uniform lift loses by construction and
only a lift landing on the cells that do fire can win. Solving that small block perfectly, with
the cross terms where they were measured, would be worth +0.0175 (update 156).

The sentence that closed the previous paragraph went further than its own decomposition did,
and this round withdraws it. For a set of precedent classes, consider every rule that leaves the
cells outside them at the general predictor's values and may give the cells inside them any real
values: that family contains every splice, every calibration, every new field and every model
restricted to those classes, so the last three rounds live inside the family of the no-precedent
class alone. Maximising a weighted AUC over a total order with one sub-sequence frozen is the
sequencing problem with chains, whose optimum is the ratio rule once the frozen chain is
compressed by pooling adjacent violators, so the maximum is exact rather than bounded. Against
0.620864 for the general predictor and 0.950170 for the per-window cell oracle, the four single
classes reach 0.732846, 0.741016, 0.720968 and 0.828781; the best pair is 0.897957; the triple
omitting the class with three or more prior isolated events reaches 0.878723, while every triple
containing it clears 0.93. So to exceed 0.90 inside this family the re-scored set must touch that
class and at least two of the other three -- necessary, not sufficient, since nothing here bounds
anything from below. The decomposition of the no-precedent optimum is what withdraws the sentence:
of its +0.111982, the block where neither cell has a precedent supplies +0.015987, or 14.3 per
cent, and the cross-class blocks supply +0.095995, or 85.7 per cent, the largest single term being
+0.051700 from pairs whose positive cell has no precedent and whose negative has three or more.
The optimum does not even take a perfect within-class block: it settles for 0.96413 and spends the
slack on the cross blocks, reaching 0.99912, 0.99836 and 0.99552, paying only -0.006905. The
cancellation reported before is therefore a property of models that cannot tell which no-precedent
cell will fire, whose lift is effectively uniform, and not of the family; a uniform lift does lose,
at a ratio of 1.999725 rather than the exact two that was written, and those masses were misnamed
-- the class holds 0.15321 of the positive pair mass and 0.26469 of the negative, while 0.111505
and 0.222980 are the two cross-block masses. An audit attacked the optimality argument itself,
since this round's own cross-check searched only orders consistent with the same rule and could
have shared one mistake with what it checked. It held: 9,765 small instances solved by enumerating
every order consistent with the frozen chain, including 1,499 whose frozen scores are the exact
reverse of ratio order, with a largest discrepancy of zero in either direction; ties as values
across 119,700 assignments gained nothing; 200,000 hill-climbing moves on real windows gained
nothing; and for all 375 window and class-set pairs an explicit score vector realising the claimed
arrangement, leaving the frozen cells in exactly the general predictor's order, measures the
reported ceiling to zero. These are attained maxima, not relaxations. The gates are recorded as
found: one assertion was true by construction; the gate freeing every class never exercises the
pooling step, since the frozen chain is then empty; the gate freeing no class catches too little
pooling but not a rule that pools everything, which passes four of six, and on that path the chain
goes from 579 groups to 7 blocks, so the ordering is 98.4 per cent non-monotone in the ratio and
the step was barely constrained; the check that does catch it covered 1.1 per cent of the
evaluations where covering all costs about a minute; no gate bounded a ceiling from above, and a
broken pooling step produced values of 1.32 while three gates passed; two reference values were
typed rather than read; the monotonicity gate is an identity of the family; and the tie-aware
accumulator names a path this data never takes, since no two cells share a score and no cell has
zero negative weight. All fifteen numbers are per-window oracles reading the labels of the window
they score, the whole-axis figure is a mean of per-window maxima, and every bound holds only while
the untouched cells keep the general predictor's exact values (update 157).

With the order inside a class prescribed and only the levels chosen, the exact maximum is again a
sequencing problem, now with two frozen chains. Keeping the general predictor's own order inside
the no-precedent class and choosing only the levels reaches 0.636983 against 0.620864. The best
order built in rounds 241 to 243 adds only +0.004696 on top, so that arc was losing on levels, not
on order. The class with three or more prior isolated events offers +0.021012 from levels alone.
An audit confirmed the maxima by enumeration over 5,618 instances and by an independent programme,
both to zero. It also withdrew one reading: within-class AUC is not a sufficient statistic. An order
correct only for the top 2 per cent of the no-precedent class reaches 0.6699 to 0.6710 on the
whole axis at a within-class AUC of about 0.69, above what the blended curve gives at that AUC. So
the reads of 0.6468 and 0.8857 belong to that curve alone. The audit also found that one gate,
covering one order, was the only guard on the new inner pooling. Chosen causally, a constant level
shift on that class gave -0.000029, shifts on all precedent classes +0.001504 (t 1.08), and a refit
on row counts -0.000742, so none meets the declared gain. Even the best constant chosen with the
labels takes only 19.8 per cent of the +0.021012. What remains lies in placing the few highest
cells of a class high, which the next round targets (update 158).

Measured instead as precision at the top of a class, lifting the true top 5 per cent of the
no-precedent class at optimal levels reaches 0.7043 on the whole axis against 0.6209. A monotone
slope-and-intercept map chosen with the labels takes only 37 to 43 per cent of the level room, so
the order at the top has to change. The general predictor's own top precision there is 0.0814 at
5 per cent and 0.1422 at 10 per cent. I first published 0.1086 and 0.3520, which broke ties in
the true top by the predictor's own score, and I withdraw them. The reason is descriptive: in that
class the positive rate is flat in the time since the last M5.5 (0.0070 to 0.0078), yet the
predictor scores recent activity higher, so its top there is active cells that do not fire. The
catalogue says little about which no-precedent cell fires. The first field from outside it,
surface heat flow from the IHFC Global Heat Flow Database 2024 (CC BY 4.0), gave -0.000125
(t -0.19) and beat 18 of 20 floor worlds, so it shows no gain (update 159).

Crustal structure from CRUST1.0 carries a signal where the catalogue does not. Inside the
no-precedent class, crustal thickness alone orders cells at 0.584 against 0.579 for the general
predictor (p 0.001 against 1,000 map permutations). The effect runs opposite ways in ocean and on
land. That split was found after looking, so it was declared and tested on cells with one or two
precedents, which had not been read. The ocean effect held (0.532, p 0.007) and the land effect did
not. A causal arm that lets the crustal columns act only where precedents are few reached 0.6220,
+0.0012, above all 20 floor worlds, but with a paired t of 0.78 over windows, so by the declared
rule it is no gain. CRUST1.0 states no licence and is used as a local input only (update 160).

Subduction geometry from Slab2 (Hayes 2018, USGS, public domain, doi 10.5066/F7PV6JNV) carries
the same kind of signal. Declared before the run, slab coverage alone orders the cells with few
precedents at 0.539 (p 0.001 against 1,000 map permutations), almost all of it in the
no-precedent class (0.575). A causal arm that lets the slab columns act only where precedents are
few reached 0.6235, +0.0026, above all 20 floor worlds, but with a paired t of 1.57 over windows,
so by the declared rule it is no gain (update 161).

A model of its own for the no-precedent class, fitted without any catalogue column on crust and
slab alone, orders that class at 0.617 against 0.579 for the general predictor, and beats a floor
that rotates the maps in longitude (31 of 31 worlds, p 0.031). On the whole axis it adds +0.0033
(t 1.58), no gain by the declared rule. Separate fits for the classes with precedents hurt them.
The general predictor's level room, +0.051 when levels are chosen per window by the labels, is
almost all per-window: a level map fixed across windows takes 6% even with future windows
(update 162).

Combining the two forms -- crust and slab acting in the classes with one or two precedents inside
the pooled fit, and the crust-and-slab model for the no-precedent class -- reaches 0.6257, +0.0049
over the general predictor with a paired t of 2.03, above all 31 rotated-map floor worlds. That
meets the declared rule for the first time in this arc, narrowly: the two-sided p is 0.053 and the
bootstrap 2.5% point is +0.0002. The arm was designed after looking at the same 25 windows, so it
is exploratory until tested on data it has not seen (update 163).

Frozen and scored on the 213 days past the last scored window, which no round had looked at, the
combined arm fell below the general predictor (0.6660 against 0.6726), mostly in the no-precedent
class. One short window is a weak test and does not refute it, but the confirmation did not come,
so the arm stays exploratory and unconfirmed (update 164).

Two clocks from the catalogue were then tried for the order inside the classes: large shocks in
the neighbouring cells over the last two years, and each well-precedented cell's own recurrence
clock. Neither adds to the general predictor (-0.0001 and +0.0005); the clock's order is mostly the
cell's activity, which the predictor already has, so the next information has to come from outside
the catalogue (update 165).

Two sources from outside the general predictor were then tried for the order inside the
well-precedented class: the surface load measured by GRACE, and the rate of earthquakes below M4.5
from the ISC bulletin. Neither adds to the general predictor (-0.0004 and -0.0002). A hindsight
check shows that the room inside that class lies in what changes from year to year, not in which
cell it is (update 166).

A gain of +0.010 inside the well-precedented class was then traced to the definition of the
target (an isolated M6 cannot occur in the shadow of an earlier large shock), so every arm is now
also scored with the label "any M6", and a gain that does not survive there is not counted. Marine
gravity, ocean sediment thickness, geology transferred from the other classes, and static Coulomb
stress from earlier large shocks all add nothing (update 167).

Tidal triggering adds nothing either. A diagnostic shows why yearly sources keep failing inside the
well-precedented class: its M6 firing has no slow year-to-year variation beyond a constant rate
per cell, so what remains is short-term, and the next step is to refresh the forecast every 34
days instead of once a year (update 168).

Refreshing the forecast every 33 days first looked like a large gain, but it was a leak from
choosing the scored cells with the future. On all cells, recent activity helps the any-M6 label
but not isolated mainshocks, and in the encodings tried neither small-quake rates nor short-term
b-value changes separate them (update 169).

The b-value change was measured again on sequences with ISC's own mb, first from the preferred
magnitudes (which removed larger events) and then from every event with all magnitudes and
completeness set where b stops changing; it does not separate isolated mainshocks in either form
(update 170).

A Japan arena was built on the JMA catalogue, where completeness is about magnitude 1.4, and a
one-year b-value change on declustered seismicity gave no gain for isolated mainshocks, though it
could be computed for only 17% of them (update 171).

On the Japan arena a wider b-value change and J-SHIS active faults gave no gain for isolated
mainshocks, and the Slab2 finding replicated on unseen Japan as whether a cell sits over a slab,
not its depth or shape, a small effect with activity held fixed (update 172).

Distance to the nearest volcano did not push down active places without isolated mainshocks, and an
oracle map placed the remaining Japan-arena gap in which cell, not in the level of the active
stratum (update 173).

A low P-wave velocity at 10 to 30 km depth (NIED model of Matsubara et al. 2022) did not mark the
places of isolated mainshocks on the Japan arena, and a Vp/Vs anomaly seen after the fact was
borderline and is not claimed (update 174).

The Vp/Vs observation was not confirmed on data it had never seen (New Zealand 2006 to 2026 with
the NZ-wide 2.3 model, and Japan 2026), in a pre-registered test of declared low power (update 175).

Crustal magnetic roughness (EMAG2v3) separated isolated mainshocks on Japan but was not confirmed on
New Zealand, and the Japan pattern traces to the arena edge and the data compilation (update 176).

The Slab2 "on a slab or not" bit was not confirmed on New Zealand, but the test had almost no
power there because nearly all positives fall in cells that are all on slab (update 177).

The three catalogue-free fields that survived the global zero-precedent screen were not confirmed on
Japan and New Zealand; what they order there is activity, not isolated events (update 178).

Causal GNSS velocity changes and transients did not order isolated M5.5 events in cells without
precedent on Japan and New Zealand (update 179).

Month-scale ionospheric TEC anomalies did not order isolated M5.5 events in cells without
precedent on Japan and New Zealand (update 180).

Month-scale sea-surface temperature anomalies did not order isolated M5.5 events in cells without
precedent on Japan and New Zealand (update 181).

Month-scale ocean-colour (chlorophyll a) anomalies did not order isolated M5.5 events in cells without
precedent on Japan and New Zealand (update 182).

Month-scale sea-surface-height anomalies did not order isolated M5.5 events in cells without
precedent on Japan and New Zealand (update 183).

Night-time magnetic-field fluctuations seen from orbit did not order isolated M5.5 events in
cells without precedent on Japan and New Zealand (update 184).

Month-scale outgoing-longwave-radiation anomalies did not order isolated M5.5 events in cells
without precedent on Japan and New Zealand (update 185).

A neural network that reads each cell's surrounding catalogue events added nothing to the
sudden axis beyond its 27 per-cell features (token-shuffle floor); the feature-only network
is frozen and scored on unseen windows, first window +0.014 over FULL, no verdict before five (update 186).

The 1918-1969 ISC-GEM record orders cells with no modern precedent beyond FULL's columns (inside that group
0.600 to 0.615, first of 62 rotated worlds) but adds under 0.001 to the whole sudden axis; the window-36 score is
reclassified as monitoring because each yearly window is now recorded once, 90 days after it closes (update 187).

A century-long elapsed-time clock (ISC-GEM 1918-1969 plus the modern record, Brownian passage time) for cells with
one or two modern precedents stays inside a within-strata permutation floor on both isolated and any-M6 labels
(gain -0.0003), so the pre-registered verdict is null and the axis does not move (update 188).

From 2026-10-20 the work ran as a separate working series of pre-registered rounds (numbered from
259 on, overlapping the numbers above). The main yardstick moved to all 2,280 cells (SUDDEN-ALL: isolated
M6 within 34 days against quiet days, no aftershock positives); on it the base rose from 0.752753 (cell
climatology) to 0.787474 (round 909; honest axis 0.777844), 33 carries in 431 seen readings against
36.5 expected under the null (update 189).

Oracle diagnostics put the distance to 0.90 in which cell-year hosts a mainshock (true cell-years with
our timing 0.9852; perfect timing with our cell order 0.7910); a cell-year instrument and the screens
on it did not move the axis, and 0.90 needs about 60% of a window's firing cells raised with at most
1% false cells, half of them in cells without land (update 190).

Existence tests in Japan's dense data (JMA, GNSS, boosted trees) found no large effect at this size
(about +0.02 at lead 1-3 days only), and on synthetic ETAS worlds a catalogue model of our type is
within about 0.01 of the true intensity, which itself reads 0.737-0.766 (update 191).

Ten non-catalogue sources (slow earthquakes, altimetry twice, GRACE gravity, TEC, Earth rotation, core
field, OLR, Swarm magnetics, OMI SO2) read with one instrument stay at |S| <= 0.101, none reaches the
pilot gate; an audit with real positive controls passes, and the gain rule 0.018 c S^2 is an upper
indication only (realised / predicted 0.02 to 5.34 over six families) (update 192).

Events below the catalogue floor read +0.300 sd at 11-34 days on Japan's CLEAN rows, but the effect
does not grow steadily with a lower floor (slope +0.042 per magnitude unit, interval includes 0), does
not carry to the global grid, and the offshore reading after the seafloor networks (+0.86 on 11
mainshocks) is not found at this size and not replicated outside Japan; it is frozen for re-reading
on new rows from 2025, and a below-floor arm is frozen for the prospective windows (update 193).

The main number has not moved since round 909 (0.787474; honest 0.777844) and no route to 0.90 is
supported by a measurement yet; 42 frozen arms were dry-run on window 36 (30 exact), the 2027 fits
start after 2026-12-15 with hashes before 2027-02-01, and new sources keep being opened as declared
pilots in the meantime (update 194).

The main number stays at 0.787474 (honest 0.777844). Rounds 992-1042 read about thirty new catalogue
parameter groups (cycle phase, loading budget, tidal sensitivity, fractal and entropy measures,
mechanism diversity, recurrence networks) and four method arms, none admitted (early gain negative in
22 of 22 groups; completeness, history length and the era difference of the model were checked), and
more than twenty further non-catalogue sources, none at its gate (26 of 1,885 readings pass their
nulls against about 94 by chance); the quiet-looking 70% of cells holds 0.306 of firing cells,
ordering every stratum perfectly gives 0.8821, 343 of 634 quiet firing cell-windows have no
small-event information, and three more arms are frozen for the 2027 fits (update 195).

The headline stays at 0.778 on the honest axis (0.777844; main axis 0.787474, on which verdicts are read).
A campaign toward 0.80 (at most eight arms, each only after a declared look-ahead room of +0.003) used 0
of 8 slots: rooms on the catalogue base are small or look-ahead only (regional firing counts +0.0076 at 8
regions, not predictable from the past or from climate indices), and transforms, trees, outside and
spatial-pattern compounds, a learned cell-graph model and three-way products show no room at this size;
eight more non-catalogue sources (full dv/v, ocean-bottom pressure, salinity, CO2, cloud fraction, radio
occultation, methane, ocean currents) fail at this size, 35 of 2,174 readings since pilot 960 pass their
nulls against about 109 by chance, and the ISC-dependent arms are fitted in 2027 through hash-line copies
(update 196).
