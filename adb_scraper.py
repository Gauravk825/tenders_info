import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

class ADBScraper:
    """
    Agent to scrape project and tender data from the ADB website with specified filters.
    """
    def __init__(self, headless=True):
        self.base_url = "https://www.adb.org"
        self.projects_url = f"{self.base_url}/projects"
        self.tenders_url = f"{self.base_url}/projects/tenders"
        self.session = requests.Session()
        self.logger = self._setup_logger()
        
        # Setup Selenium
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        self.logger.info("Initializing webdriver...")
        self.driver = webdriver.Chrome(options=options)
        self.logger.info("Webdriver initialized")
        
    def _setup_logger(self):
        logger = logging.getLogger('ADBScraper')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def search_tenders(self, filters=None):
        """
        Search for tenders with the specified filters.
        
        Args:
            filters (dict): Dictionary of filters to apply. Example:
                {
                    'country': 'India',
                    'status': 'Active',
                    'sector': 'Water and other urban infrastructure and services'
                }
        
        Returns:
            pd.DataFrame: DataFrame containing the search results
        """
        self.logger.info(f"Searching tenders with filters: {filters}")
        
        try:
            # Navigate to tenders page
            self.driver.get(self.tenders_url)
            self.logger.info("Navigated to tenders page")
            
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".filter-results"))
            )
            
            # Apply country filter
            if filters and 'country' in filters:
                self._apply_filter("Country/Economy", filters['country'])
            
            # Apply sector filter
            if filters and 'sector' in filters:
                self._apply_filter("Sectors", filters['sector'])
            
            # Apply status filter - for tenders, this is usually in the search results
            # For tenders, status filters are typically shown as tags in the results section
            
            # Wait for results to load
            time.sleep(2)
            
            # Extract results
            results = self._extract_tender_results()
            
            # Filter by status if needed (some sites require post-filtering)
            if filters and 'status' in filters:
                results = results[results['Status'] == filters['status']]
            
            self.logger.info(f"Found {len(results)} tenders matching the criteria")
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching tenders: {str(e)}")
            return pd.DataFrame()
    
    def _apply_filter(self, filter_category, filter_value):
        """Apply a filter in the UI"""
        try:
            # Find the filter category section
            category_elements = self.driver.find_elements(By.XPATH, f"//div[contains(text(), '{filter_category}')]")
            
            if not category_elements:
                self.logger.warning(f"Filter category '{filter_category}' not found")
                return
                
            category_element = category_elements[0]
            
            # Expand the filter section if collapsed
            parent_section = category_element.find_element(By.XPATH, "./..")
            if "collapsed" in parent_section.get_attribute("class"):
                category_element.click()
                time.sleep(0.5)
            
            # Find and click the checkbox for the filter value
            checkbox = self.driver.find_element(
                By.XPATH, 
                f"//label[contains(text(), '{filter_value}')]/preceding-sibling::input[@type='checkbox']"
            )
            if not checkbox.is_selected():
                checkbox.click()
                
            # Wait for the page to update with new results
            time.sleep(2)
            
            self.logger.info(f"Applied filter: {filter_category} = {filter_value}")
        except Exception as e:
            self.logger.error(f"Error applying filter {filter_category}={filter_value}: {str(e)}")
    
    def _extract_tender_results(self):
        """Extract tender results from the page"""
        self.logger.info("Extracting tender results...")
        results = []
        
        # Get all tender items
        tender_items = self.driver.find_elements(By.CSS_SELECTOR, "div.tender-item")
        self.logger.info(f"Found {len(tender_items)} tender items")
        
        for item in tender_items:
            try:
                # Extract data from each tender item
                title_element = item.find_element(By.CSS_SELECTOR, "h3.title a")
                title = title_element.text.strip()
                link = title_element.get_attribute("href")
                
                # Extract other metadata
                metadata = {}
                metadata_elements = item.find_elements(By.CSS_SELECTOR, "div.metadata div")
                
                for element in metadata_elements:
                    text = element.text.strip()
                    if ":" in text:
                        key, value = text.split(":", 1)
                        metadata[key.strip()] = value.strip()
                
                # Get status
                status = "Unknown"
                status_elements = item.find_elements(By.CSS_SELECTOR, "div.status")
                if status_elements:
                    status = status_elements[0].text.strip()
                
                # Create result entry
                result = {
                    "Title": title,
                    "Link": link,
                    "Status": status,
                    **metadata
                }
                
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Error extracting tender item: {str(e)}")
        
        self.logger.info(f"Extracted {len(results)} tender results")
        return pd.DataFrame(results)
    
    def search_projects(self, filters=None):
        """
        Search for projects with the specified filters.
        
        Args:
            filters (dict): Dictionary of filters to apply. Example:
                {
                    'country': 'India',
                    'status': 'Proposed',
                    'sector': 'Energy'
                }
        
        Returns:
            pd.DataFrame: DataFrame containing the search results
        """
        self.logger.info(f"Searching projects with filters: {filters}")
        
        try:
            # Navigate to projects page
            self.driver.get(self.projects_url)
            self.logger.info("Navigated to projects page")
            
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".filter-results"))
            )
            
            # Apply filters
            if filters:
                for filter_type, filter_value in filters.items():
                    if filter_type.lower() == 'country':
                        self._apply_filter("Country/Economy", filter_value)
                    elif filter_type.lower() == 'sector':
                        self._apply_filter("Sectors", filter_value)
                    # Status is usually displayed in the results
            
            # Wait for results to load
            time.sleep(2)
            
            # Extract results
            results = self._extract_project_results()
            
            # Filter by status if needed
            if filters and 'status' in filters:
                results = results[results['Status'] == filters['status']]
            
            self.logger.info(f"Found {len(results)} projects matching the criteria")
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching projects: {str(e)}")
            return pd.DataFrame()
    
    def _extract_project_results(self):
        """Extract project results from the page"""
        results = []
        
        # Get all project items
        project_items = self.driver.find_elements(By.CSS_SELECTOR, "div.project-item")
        
        for item in project_items:
            try:
                # Extract data from each project item
                title_element = item.find_element(By.CSS_SELECTOR, "h3.title a")
                title = title_element.text.strip()
                link = title_element.get_attribute("href")
                
                # Extract project ID and country
                project_details = item.find_element(By.CSS_SELECTOR, "div.project-id").text.strip()
                
                # Extract status
                status = "Unknown"
                status_elements = item.find_elements(By.CSS_SELECTOR, "div.status")
                if status_elements:
                    status = status_elements[0].text.strip()
                
                # Extract approval year
                approval_year = "Unknown"
                year_elements = item.find_elements(By.CSS_SELECTOR, "div.year")
                if year_elements:
                    approval_year = year_elements[0].text.strip()
                
                # Create result entry
                result = {
                    "Title": title,
                    "Link": link,
                    "Project Details": project_details,
                    "Status": status,
                    "Approval Year": approval_year
                }
                
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Error extracting project item: {str(e)}")
        
        return pd.DataFrame(results)
    
    def _extract_project_results(self):
        """Extract project results from the page"""
        self.logger.info("Extracting project results...")
        results = []
        
        # Get all project items
        project_items = self.driver.find_elements(By.CSS_SELECTOR, "div.project-item")
        self.logger.info(f"Found {len(project_items)} project items")
        
        for item in project_items:
            try:
                # Extract data from each project item
                title_element = item.find_element(By.CSS_SELECTOR, "h3.title a")
                title = title_element.text.strip()
                link = title_element.get_attribute("href")
                
                # Extract project ID and country
                project_details = item.find_element(By.CSS_SELECTOR, "div.project-id").text.strip()
                
                # Extract status
                status = "Unknown"
                status_elements = item.find_elements(By.CSS_SELECTOR, "div.status")
                if status_elements:
                    status = status_elements[0].text.strip()
                
                # Extract approval year
                approval_year = "Unknown"
                year_elements = item.find_elements(By.CSS_SELECTOR, "div.year")
                if year_elements:
                    approval_year = year_elements[0].text.strip()
                
                # Create result entry
                result = {
                    "Title": title,
                    "Link": link,
                    "Project Details": project_details,
                    "Status": status,
                    "Approval Year": approval_year
                }
                
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Error extracting project item: {str(e)}")
        
        self.logger.info(f"Extracted {len(results)} project results")
        return pd.DataFrame(results)
    
    def get_tender_details(self, tender_url):
        """
        Get detailed information about a specific tender.
        Args:
            tender_url (str): URL of the tender page
            
        Returns:
            dict: Dictionary containing tender details
        """
        self.logger.info(f"Getting details for tender: {tender_url}")
        
        try:
            self.driver.get(tender_url)
            
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.tender-details"))
            )
            
            # Extract tender details
            details = {}
            
            # Extract title
            title_element = self.driver.find_element(By.CSS_SELECTOR, "h1.page-title")
            details["Title"] = title_element.text.strip()
            
            # Extract metadata fields
            metadata_items = self.driver.find_elements(By.CSS_SELECTOR, "div.metadata-item")
            for item in metadata_items:
                try:
                    label = item.find_element(By.CSS_SELECTOR, "div.label").text.strip()
                    value = item.find_element(By.CSS_SELECTOR, "div.value").text.strip()
                    details[label] = value
                except Exception:
                    pass
            
            # Extract description if available
            try:
                description = self.driver.find_element(By.CSS_SELECTOR, "div.description").text.strip()
                details["Description"] = description
            except Exception:
                details["Description"] = ""
            
            self.logger.info(f"Retrieved details for tender: {details.get('Title', 'Unknown')}")
            return details
            
        except Exception as e:
            self.logger.error(f"Error getting tender details: {str(e)}")
            return {}
    
    def get_project_details(self, project_url):
        """
        Get detailed information about a specific project.
        Args:
            project_url (str): URL of the project page
            
        Returns:
            dict: Dictionary containing project details
        """
        self.logger.info(f"Getting details for project: {project_url}")
        
        try:
            self.driver.get(project_url)
            
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.project-details"))
            )
            
            # Extract project details
            details = {}
            
            # Extract title
            title_element = self.driver.find_element(By.CSS_SELECTOR, "h1.page-title")
            details["Title"] = title_element.text.strip()
            
            # Extract metadata fields
            metadata_items = self.driver.find_elements(By.CSS_SELECTOR, "div.metadata-item")
            for item in metadata_items:
                try:
                    label = item.find_element(By.CSS_SELECTOR, "div.label").text.strip()
                    value = item.find_element(By.CSS_SELECTOR, "div.value").text.strip()
                    details[label] = value
                except Exception:
                    pass
            
            # Extract description if available
            try:
                description = self.driver.find_element(By.CSS_SELECTOR, "div.description").text.strip()
                details["Description"] = description
            except Exception:
                details["Description"] = ""
            
            self.logger.info(f"Retrieved details for project: {details.get('Title', 'Unknown')}")
            return details
            
        except Exception as e:
            self.logger.error(f"Error getting project details: {str(e)}")
            return {}
    
    def save_results_to_csv(self, results, filename):
        """
        Save results to a CSV file.
        
        Args:
            results (pd.DataFrame): Results to save
            filename (str): Name of the CSV file
        """
        try:
            results.to_csv(filename, index=False)
            self.logger.info(f"Results saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving results to CSV: {str(e)}")
    
    def close(self):
        """Close the browser and clean up resources"""
        try:
            self.driver.quit()
            self.logger.info("Browser closed")
        except Exception as e:
            self.logger.error(f"Error closing browser: {str(e)}")


# Example usage
if __name__ == "__main__":
    # Initialize scraper
    scraper = ADBScraper(headless=False)  # Set to True for headless mode
    
    try:
        # Search for tenders with filters
        filters = {
            'country': 'India',
            'status': 'Active',
            'sector': 'Water and other urban infrastructure and services'
        }
        
        # Get tender results
        tender_results = scraper.search_tenders(filters)
        
        # Save results to CSV
        if not tender_results.empty:
            scraper.save_results_to_csv(tender_results, "adb_tenders.csv")
            
            # Get details for the first 5 tenders
            for i, row in tender_results.head(5).iterrows():
                details = scraper.get_tender_details(row['Link'])
                print(f"Details for tender {i+1}:")
                for key, value in details.items():
                    print(f"  {key}: {value}")
                print()
        
    finally:
        # Clean up
        scraper.close()
