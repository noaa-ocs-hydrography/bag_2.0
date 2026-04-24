# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 15:40:58 2025

@author: Anthony.R.Klemm
"""
import sys
import os
import subprocess


# TODO Pydro is missing bagPy.  This force install sit in current environment
def install_bagpy_library():
    command = [sys.executable, '-m', 'conda', 'install', '-c', 'conda-forge', 'bagpy=2.0.1']
    subprocess.check_call(command)
    print('Installed bagPy v2.0.1')
install_bagpy_library()


from PySide6.QtCore import QObject, QThread, Signal, Slot, Qt
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QPushButton, QLineEdit, QFileDialog, QListWidget,
    QListWidgetItem, QPlainTextEdit, QMessageBox
)

from bag_processor import process_bags


class Worker(QObject):
    """
    A worker object that runs a task in a separate thread.
    Emits signals to communicate with the main GUI thread.
    """
    progress = Signal(str)
    finished = Signal()
    error = Signal(str)

    def __init__(self, data_layers, output_path):
        super().__init__()
        self.data_layers = data_layers
        self.output_path = output_path

    @Slot()
    def run(self):
        """Execute the long-running task."""
        try:
            # The process_bags function's status_callback is connected to this worker's
            # progress signal. The .emit sends the message to the main thread.
            process_bags(self.data_layers, self.output_path, status_callback=self.progress.emit)
        except Exception as e:
            self.error.emit(f"A critical error occurred: {e}")
        finally:
            self.finished.emit()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BAG 2.x Converter (PySide6)")
        self.setGeometry(100, 100, 800, 600)

        # --- Main Layout ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # --- Input Layers Group ---
        layers_group = QGroupBox("Input Data Layers (add high-precedence last)")
        layers_layout = QVBoxLayout()
        self.layer_list = QListWidget()
        layers_layout.addWidget(self.layer_list)

        # Buttons for adding/removing layers
        layer_buttons_layout = QHBoxLayout()
        add_layer_btn = QPushButton("+ Add Layer")
        add_layer_btn.clicked.connect(self.add_layer)
        remove_layer_btn = QPushButton("- Remove Selected Layer")
        remove_layer_btn.clicked.connect(self.remove_layer)
        layer_buttons_layout.addWidget(add_layer_btn)
        layer_buttons_layout.addWidget(remove_layer_btn)
        layers_layout.addLayout(layer_buttons_layout)
        layers_group.setLayout(layers_layout)

        # --- Output File Group ---
        output_group = QGroupBox("Output File")
        output_layout = QHBoxLayout()
        self.output_path_edit = QLineEdit()
        self.output_path_edit.setPlaceholderText("Select output .bag file path...")
        browse_output_btn = QPushButton("Browse...")
        browse_output_btn.clicked.connect(self.browse_output)
        output_layout.addWidget(self.output_path_edit)
        output_layout.addWidget(browse_output_btn)
        output_group.setLayout(output_layout)

        # --- Run Button ---
        self.run_button = QPushButton("Run Conversion")
        self.run_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                font-size: 14px;
                font-weight: bold;
                padding: 15px 20px;
                border-radius: 5px; /* Rounded corners */
                border: none;
            }
            QPushButton:hover {
                background-color: #45a045; /* A slightly darker green on hover */
            }
        """)
        # self.run_button.setStyleSheet("font-size: 14px; padding: 10px;")
        self.run_button.clicked.connect(self.run_conversion)

        # --- Log Group ---
        log_group = QGroupBox("Log")
        log_layout = QVBoxLayout()
        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        log_group.setLayout(log_layout)
        
        # --- Add all widgets to main layout ---
        main_layout.addWidget(layers_group)
        main_layout.addWidget(output_group)
        main_layout.addWidget(self.run_button, alignment=Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(log_group, stretch=1) # Give log area extra space

    def add_layer(self):
        """Dialog to add a new BAG/XML layer."""
        bag_path, _ = QFileDialog.getOpenFileName(self, "Select BAG Data File", "", "BAG Files (*.bag)")
        if not bag_path:
            return

        xml_path, _ = QFileDialog.getOpenFileName(self, "Select Survey Metadata XML", "", "XML Files (*.xml)")
        if not xml_path:
            return

        layer_name = "non-interpolated" if "interp" not in bag_path.lower() else "interpolated"
        
        # Store the data in a QListWidgetItem
        item = QListWidgetItem(f"[{layer_name}] {os.path.basename(bag_path)}")
        item.setData(1, {'name': layer_name, 'data_path': bag_path, 'metadata_path': xml_path}) # Use a role to store data
        self.layer_list.addItem(item)

    def remove_layer(self):
        """Removes the currently selected layer."""
        current_item = self.layer_list.currentItem()
        if current_item:
            self.layer_list.takeItem(self.layer_list.row(current_item))

    def browse_output(self):
        """Dialog to select the output file path."""
        output_path, _ = QFileDialog.getSaveFileName(self, "Save Output File", "", "BAG Files (*.bag)")
        if output_path:
            self.output_path_edit.setText(output_path)

    @Slot(str)
    def append_log(self, message):
        """Appends a message to the log text area."""
        self.log_text.appendPlainText(message)

    # def set_controls_enabled(self, enabled):
    #     """Enable or disable UI controls during processing."""
    #     self.run_button.setEnabled(enabled)
    #     self.layer_list.setEnabled(enabled)
        
    def on_worker_finished(self):
        """Actions to take when the thread is finished."""
        self.append_log("--- Process Finished ---")
        # self.set_controls_enabled(True)
        # Clean up the thread and worker
        self.thread.quit()
        self.worker.deleteLater()
        self.thread.deleteLater()

    def run_conversion(self):
        """Prepares and starts the background processing task."""
        # 1. Gather data from the UI
        output_path = self.output_path_edit.text()
        data_layers = []
        for i in range(self.layer_list.count()):
            item = self.layer_list.item(i)
            layer_data = item.data(1)
            layer_data['key'] = i + 1
            data_layers.append(layer_data)

        # 2. Validate input
        if not data_layers:
            QMessageBox.warning(self, "Input Error", "Please add at least one data layer.")
            return
        if not output_path:
            QMessageBox.warning(self, "Input Error", "Please specify an output file path.")
            return

        # 3. Set up and run the background thread
        # self.set_controls_enabled(False)
        self.log_text.clear()
        self.append_log("--- Starting Conversion ---")

        self.thread = QThread()
        self.worker = Worker(data_layers=data_layers, output_path=output_path)
        self.worker.moveToThread(self.thread)

        # Connect signals from the worker to slots in the main GUI
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_worker_finished)
        self.worker.progress.connect(self.append_log)
        self.worker.error.connect(lambda msg: QMessageBox.critical(self, "Processing Error", msg))
        
        self.thread.start()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())