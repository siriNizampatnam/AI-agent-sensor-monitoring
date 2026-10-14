# AI Agent Sensor Monitoring System

## Project Overview

The **AI Agent Sensor Monitoring System** is a real-time sensor monitoring application designed to collect, process, visualize, and analyze sensor data using an agent-based architecture. The system enables users to monitor sensor readings, identify threshold violations, receive alerts, and generate reports through a web-based interface.

The application uses **React** for the frontend and **FastAPI** for the backend. It supports sensor data processing from JSON files and real-time data streaming using MQTT and WebSockets. The system is designed with modular agents, each responsible for a specific monitoring task.

## Key Features

- **Dynamic Sensor Monitoring:** Automatically identifies available sensors and displays their readings.
- **Live Data Streaming:** Receives real-time sensor updates through MQTT and WebSockets.
- **Sensor Data Visualization:** Displays sensor readings in tabular and graphical formats.
- **Threshold-Based Alerts:** Detects sensor values that cross configured thresholds and supports alert notifications.
- **Report Generation:** Provides a dedicated report interface for monitoring results and report-related operations.
- **Agent-Based Architecture:** Separates monitoring, alert handling, and reporting responsibilities into individual agents.
- **Interactive Dashboard:** Provides a user-friendly interface for viewing and analyzing sensor data.

## Technology Stack

- **Frontend:** React, JavaScript, Material UI
- **Backend:** Python, FastAPI
- **Communication:** REST APIs, WebSockets
- **Messaging:** MQTT
- **Data Format:** JSON
- **Agent Integration:** Model Context Protocol (MCP)

## How It Works

1. The system loads sensor data from JSON files or receives live readings through MQTT.
2. The backend processes the data and identifies available sensors dynamically.
3. The frontend displays sensor readings in a list view and visualizes them using graphs.
4. The monitoring logic checks sensor values against configured thresholds.
5. When a threshold-crossing condition is detected, the alert functionality can notify the configured recipient.
6. The reporting component provides a separate interface for generating or accessing monitoring reports as implemented.

## Project Objective

The objective of this project is to simplify sensor monitoring by combining real-time data processing, visualization, threshold-based alerting, and modular agent functionality in a single application. It aims to make sensor information easier to monitor, understand, and manage.

## Future Enhancements

- Add authentication and role-based access control.
- Improve alert configuration and notification management.
- Complete automated report generation and PDF export.
- Add historical data storage and advanced analytics.
- Expand automated testing and monitoring reliability.

## Author

**Sai Siri Nizampatnam**
