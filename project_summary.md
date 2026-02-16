# Regatta Data Orchestration Platform: Project Summary

This document provides a comprehensive summary of the Regatta Data Orchestration Platform project, suitable for import into NotebookLM.

## 1. Project Overview

The Regatta Data Orchestration Platform is a system designed to consolidate and manage data related to sailboat regattas. It aims to provide a single source of truth for information about yacht clubs, yachts, participants, and events. The platform will be used to drive event participation, manage documentation, and support marketing campaigns.

## 2. Problem Statement

Regatta organizers currently lack a unified system to track and manage data across various sources, including registration systems, waiver forms, and scraped websites. This makes it difficult to get a complete picture of event participation and to effectively target outreach efforts.

## 3. Goals

The primary goal of the platform is to provide reliable, queryable intelligence to:

*   Increase event participation.
*   Streamline the management of required documentation.
*   Create targeted audiences for marketing campaigns.

## 4. Scope

*   **In Scope**: Data consolidation, historical tracking, "next best action" recommendations, and storage of large artifacts.
*   **Out of Scope**: Registration form hosting, payment processing, and direct outbound messaging.

## 5. Architecture

The platform is built on Google Cloud Platform (GCP) and consists of the following components:

*   **Cloud SQL for PostgreSQL**: The primary database for relational data.
*   **Google Cloud Storage (GCS)**: For storing large files and raw data.
*   **Cloud Run**: For running containerized applications and APIs.
*   **Cloud Scheduler and Pub/Sub**: For scheduling and asynchronous tasks.
*   **Secret Manager**: For managing secrets and credentials.
*   **Cloud Logging and Cloud Monitoring**: For observability.

## 6. Data Model

The data model is designed to be relational and is stored in a PostgreSQL database. The key entities are:

*   `yacht_club`: Information about yacht clubs.
*   `participant`: Information about individuals, including owners and crew.
*   `yacht`: Information about sailboats.
*   `event_series`: Represents a recurring event.
*   `event_instance`: Represents a specific occurrence of an event in a given year.
*   `event_entry`: A yacht's registration in an event.
*   `event_entry_participant`: The people associated with a yacht's entry in an event.
*   `document_status`: Tracks the status of required documents.
*   `next_best_action`: Stores recommendations for actions to be taken.

## 7. Key Functionality

*   **Data Ingestion**: The platform will ingest data from various sources, including `regattaman.com`, `Jotform`, and scraped data from `yachtscoring.com`.
*   **Entity Resolution**: The system will identify and merge duplicate records for participants, yachts, and clubs.
*   **Historical Tracking**: The platform will maintain a historical record of ownership, membership, and participation.
*   **Next Best Action Engine**: A rule-based engine will generate recommendations for actions, such as contacting likely participants who have not yet registered.
*   **Audience Segmentation**: The platform will provide data to create targeted audiences for marketing campaigns.
