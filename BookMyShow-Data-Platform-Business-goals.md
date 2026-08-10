# BookMyShow — Data Platform Business Problem Statement

## Background

BookMyShow is India's largest online movie and event ticketing platform,
processing bookings across hundreds of theatres and thousands of shows every
day, spanning multiple cities, payment gateways, and booking channels (web,
mobile app, box office).

Right now, the data that describes this business — bookings, shows, movies,
theatres, and customers — exists only as raw operational data. There is no
governed, trustworthy analytics layer on top of it. As a result, leadership
and operations teams cannot reliably answer basic questions about the health
of the business without going back to engineering teams for one-off queries.

## The Business Problem

Three issues are recurring and costly:

1. **No unified view of revenue and booking performance.** Revenue, ticket
   volume, and occupancy numbers live in disconnected sources and are
   reconciled manually, which is slow and error-prone. Leadership cannot see
   day-to-day trends, city-level performance, or which movies are actually
   driving revenue without waiting on ad-hoc reports.

2. **Payment failures go undetected until customers complain.** BookMyShow
   works with multiple third-party payment gateways. When a gateway degrades
   or a regional outage occurs, there is currently no systematic way to
   detect that failures have spiked for a specific gateway, city, or time
   window — the business only finds out after customer complaints and lost
   revenue have already piled up. A recent incident where a payment gateway
   silently failed for an entire day in two major cities went unnoticed for
   hours, directly costing bookings and revenue.

3. **No single source of truth for customer and theatre performance.**
   Customer loyalty/spend behavior, theatre occupancy, and movie performance
   (hit vs. underperformer) cannot currently be measured consistently — every
   team calculates these numbers differently, so reports don't agree with
   each other.

## The Ask

We need a data platform that ingests the raw booking ecosystem data, cleans
and models it, and exposes a trustworthy, governed set of KPIs — so that
business and operations teams can monitor performance daily and catch
issues like payment gateway failures as they happen, not after the fact.

**Data to be processed** (produced continuously by the booking platform):
- **Theatres** — theatre details, city/state, screen types, seat capacity.
- **Movies** — title, genre, duration, release details.
- **Customers** — customer profile and lifetime spend behavior.
- **Shows** — showtimes per movie/theatre, screen type, base ticket pricing.
- **Bookings** — the core transactional record: seats booked, ticket amount,
  fees, discounts, payment gateway, payment status (success/failed/refunded),
  booking channel, cancellations.

Expected volume: on the order of hundreds of thousands of shows and millions
of bookings, growing daily, and needs to support both a full historical
rebuild and an incremental daily refresh.

## Required KPIs

- **Total Revenue** and **Daily Revenue Trend**
- **Total Bookings**, **Successful Bookings**, **Failed Bookings**
- **Average Ticket Price**
- **Payment Failure Rate** — overall, and broken down by payment gateway,
  city, and date, so a localized or gateway-specific spike is immediately
  traceable rather than hidden inside an overall average
- **Cancellation Rate**
- **Unique Customers** and **customer loyalty segmentation** based on
  lifetime spend
- **Movie performance classification** (hit / steady / underperforming)
  based on revenue
- **Theatre occupancy rate**, by theatre and screen type
- **Revenue by city** and **booking channel split**
- **Top-performing movies by revenue**

## What We Need From a Data Engineering Solution

- A reliable way to ingest the raw booking data continuously and durably,
  without losing or duplicating records.
- Data quality enforcement so downstream reports can be trusted without
  every team re-validating the numbers themselves.
- A clean, well-modeled layer (clear entities and relationships) that
  supports fast, consistent reporting.
- One governed definition for each KPI above, used consistently everywhere
  it's reported — not recalculated differently by every team.
- A dashboard that gives leadership an at-a-glance view of business health,
  and is specifically able to surface a payment gateway/city/date failure
  spike clearly enough that it would be caught immediately rather than
  discovered through customer complaints.
- A repeatable, automated way to keep this refreshed daily, and a controlled
  process for moving changes from development into production safely.

## Success Looks Like

- Leadership can check one dashboard and immediately understand revenue,
  booking, and occupancy trends without asking engineering for a custom
  report.
- A payment gateway outage affecting a specific city and date is visible
  within the reporting layer immediately, broken down clearly enough to
  identify the responsible gateway, city, and time window.
- Every team reports the same numbers for revenue, failure rate, and
  occupancy, because those metrics are defined once and reused everywhere.
- The whole platform can be refreshed on a schedule with no manual
  intervention, and confidently redeployed from a clean environment.
