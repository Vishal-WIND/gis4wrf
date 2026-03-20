# GIS4WRF (https://doi.org/10.5281/zenodo.1288569)
# Copyright (c) 2018 D. Meyer and M. Riechert. Licensed under MIT.

from PyQt5.QtWidgets import QDockWidget, QTabWidget
from qgis.gui import QgisInterface
import os

from gis4wrf.plugin.ui.tab_home import HomeTab
from gis4wrf.plugin.ui.tab_datasets import DatasetsTab
from gis4wrf.plugin.ui.tab_simulation import SimulationTab
from gis4wrf.plugin.ui.widget_view import ViewWidget
from gis4wrf.plugin.ui.helpers import WhiteScroll


class MainDock(QDockWidget):
    """Set up the principle side dock"""
    def __init__(self, iface: QgisInterface, dock_widget: QDockWidget) -> None:
        super().__init__('GIS4WRF')

        # Give this dock a stable object name so the QSS can target it
        self.setObjectName('g4wDock')

        tabs = QTabWidget()
        tabs.addTab(WhiteScroll(HomeTab(iface)), 'Home')
        tabs.addTab(DatasetsTab(iface), "Datasets")
        self.simulation_tab = SimulationTab(iface)
        tabs.addTab(self.simulation_tab, "Simulation")
        self.view_tab = ViewWidget(iface, dock_widget)
        tabs.addTab(self.view_tab, "View")
        self.setWidget(tabs)
        self.tabs = tabs

        # Load custom stylesheet with softer colors / better contrast
        style_path = os.path.join(os.path.dirname(__file__), 'soft_colors.qss')
        if os.path.exists(style_path):
            with open(style_path, encoding='utf-8') as f:
                self.setStyleSheet(f.read())

        self.simulation_tab.view_wrf_nc_file.connect(self.view_wrf_nc_file)
