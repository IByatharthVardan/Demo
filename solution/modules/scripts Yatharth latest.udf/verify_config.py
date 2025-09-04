

import json
import sys
from typing import Dict, List, Any, Set

class ConfigVerificationError(Exception):
    """Custom exception for configuration verification errors."""
    def __init__(self, errors: List[str]):
        self.errors = errors
        error_message = f"Configuration verification failed with {len(errors)} error(s):\n"
        for i, error in enumerate(errors, 1):
            error_message += f"  {i}. {error}\n"
        super().__init__(error_message)

class ConfigVerifier:
    def __init__(self, ibfile, config_pat):
        self.config_path = config_path
        self._ibfile = ibfile
        self.config = None
        self.errors = []
        self.warnings = []
        
    def load_config(self) -> bool:
        """Load and parse the config file."""
        try:
            fc, _ = self._ibfile.read_file(self.config_path)
            self.config = json.loads(fc)
            return True
        except FileNotFoundError:
            self.errors.append(f"Config file '{self.config_path}' not found")
            return False
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON in config file: {e}")
            return False
        except Exception as e:
            self.errors.append(f"Error loading config file: {e}")
            return False
    
    def _check_errors_and_raise(self, step_name: str) -> None:
        """Check if there are any errors and raise ConfigVerificationError if found."""
        if self.errors:
            print(f"{step_name} failed with {len(self.errors)} error(s)")
            raise ConfigVerificationError(self.errors)
    
    def verify_structure(self) -> None:
        """Verify the basic structure of the config."""
        if self.config is None:
            self.errors.append("Config is None")
            return
            
        required_sections = ["data_not_present", "available_classes", "class_priority_lists", "fields", "default_field_config"]
        
        for section in required_sections:
            if section not in self.config:
                self.errors.append(f"Missing required section: '{section}'")
        
        # Check if sections are the expected type
        if "data_not_present" in self.config and not isinstance(self.config["data_not_present"], dict):
            self.errors.append("'data_not_present' should be a dictionary")
            
        if "available_classes" in self.config and not isinstance(self.config["available_classes"], list):
            self.errors.append("'available_classes' should be a list")
            
        if "class_priority_lists" in self.config and not isinstance(self.config["class_priority_lists"], dict):
            self.errors.append("'class_priority_lists' should be a dictionary")
            
        if "fields" in self.config and not isinstance(self.config["fields"], dict):
            self.errors.append("'fields' should be a dictionary")
            
        if "default_field_config" in self.config and not isinstance(self.config["default_field_config"], dict):
            self.errors.append("'default_field_config' should be a dictionary")
    
    def verify_data_not_present(self) -> None:
        """Verify the data_not_present section."""
        if self.config is None or "data_not_present" not in self.config:
            return
            
        data_not_present = self.config["data_not_present"]
        if not data_not_present:
            self.warnings.append("'data_not_present' section is empty")
            
        # Check that all values are strings
        for key, value in data_not_present.items():
            if not isinstance(value, str):
                self.errors.append(f"Value for '{key}' in data_not_present should be a string, got {type(value)}")
    

    def verify_available_classes(self) -> List[str]:
        """Verify the available_classes section"""
        
        if self.config is None or "available_classes" not in self.config:
            self.errors.append("'available_classes' section is missing")
            return []
        
        available_classes = self.config["available_classes"]
        if not isinstance(available_classes, list):
            self.errors.append("'available_classes' should be a list")
            return []

        return available_classes
            
    def verify_class_priority_lists(self, available_classes: List[str]) -> Set[str]:
        """Verify the class_priority_lists section and return available priorities"""
        if self.config is None or "class_priority_lists" not in self.config:
            self.errors.append("'class_priority_lists' section is missing")
            return set()
        
        class_priority_lists = self.config["class_priority_lists"]
        if not isinstance(class_priority_lists, dict):
            self.errors.append("'class_priority_lists' should be a dictionary")
            return set()

        available_priorities = set(class_priority_lists.keys())
        
        # Verify that all classes in priority lists exist in available_classes
        for class_priority, class_priority_list in class_priority_lists.items():
            if not isinstance(class_priority_list, list):
                self.errors.append(f"Class priority list '{class_priority}' should be a list")
                continue
                
            for class_name in class_priority_list:
                if class_name not in available_classes:
                    self.errors.append(f"Class Priority list '{class_priority}' contains invalid class '{class_name}'")
        
        return available_priorities
        
    
    def verify_default_field_config(self) -> Dict[str, Any]:
        """Verify the default field configuration."""
        if "default_field_config" not in self.config:
            return {}
            
        default_config = self.config["default_field_config"]
        valid_field_properties = {
            "human_review", "class_priority", "table", "empty", 
            "merge_type", "intra_merge_type", "sep"
        }
        
        # Check for unknown properties
        for prop in default_config:
            if prop not in valid_field_properties:
                self.warnings.append(f"Unknown property '{prop}' in default_field_config")
        
        # Validate property types
        if "human_review" in default_config and not isinstance(default_config["human_review"], bool):
            self.errors.append("'human_review' in default_field_config should be boolean")
            
        if "table" in default_config and not isinstance(default_config["table"], bool):
            self.errors.append("'table' in default_field_config should be boolean")
            
        if "class_priority" in default_config and not isinstance(default_config["class_priority"], str):
            self.errors.append("'class_priority' in default_field_config should be string")
            
        for str_prop in ["empty", "merge_type", "intra_merge_type", "sep"]:
            if str_prop in default_config and not isinstance(default_config[str_prop], str):
                self.errors.append(f"'{str_prop}' in default_field_config should be string")
        
        return default_config
    
    def verify_fields(self, available_priorities: Set[str], available_empty_keys: Set[str]) -> None:
        """Verify field configurations."""
        if self.config is None or "fields" not in self.config:
            return
            
        fields = self.config["fields"]
        valid_field_properties = {
            "human_review", "class_priority", "table", "empty", 
            "merge_type", "intra_merge_type", "sep"
        }
        
        for field_name, field_config in fields.items():
            if not isinstance(field_config, dict):
                self.errors.append(f"Field '{field_name}' should be a dictionary")
                continue
            
            # Check for unknown properties
            for prop in field_config:
                if prop not in valid_field_properties:
                    self.warnings.append(f"Unknown property '{prop}' in field '{field_name}'")
            
            # Validate property types
            if "human_review" in field_config and not isinstance(field_config["human_review"], bool):
                self.errors.append(f"'human_review' in field '{field_name}' should be boolean")
                
            if "table" in field_config and not isinstance(field_config["table"], bool):
                self.errors.append(f"'table' in field '{field_name}' should be boolean")
            
            # Validate class_priority reference
            if "class_priority" in field_config:
                class_priority = field_config["class_priority"]
                if not isinstance(class_priority, str):
                    self.errors.append(f"'class_priority' in field '{field_name}' should be string")
                elif class_priority not in available_priorities:
                    self.errors.append(f"Field '{field_name}' references unknown class_priority '{class_priority}'")
            
            # Validate empty reference
            if "empty" in field_config:
                empty_ref = field_config["empty"]
                if not isinstance(empty_ref, str):
                    self.errors.append(f"'empty' in field '{field_name}' should be string")
                elif empty_ref not in available_empty_keys:
                    self.errors.append(f"Field '{field_name}' references unknown empty key '{empty_ref}'")
            
            # Validate string properties
            for str_prop in ["merge_type", "intra_merge_type", "sep"]:
                if str_prop in field_config and not isinstance(field_config[str_prop], str):
                    self.errors.append(f"'{str_prop}' in field '{field_name}' should be string")
    
    def verify_hidden_fields(self) -> None:
        """Verify hidden fields configuration."""
        if "hidden" not in self.config:
            return
            
        hidden = self.config["hidden"]
        
        if "fields" in hidden:
            hidden_fields = hidden["fields"]
            if not isinstance(hidden_fields, list):
                self.errors.append("'hidden.fields' should be a list")
            else:
                for i, field in enumerate(hidden_fields):
                    if not isinstance(field, str):
                        self.errors.append(f"Hidden field {i} should be a string, got {type(field)}")
    
    def verify_consistency(self, available_priorities: Set[str]) -> None:
        """Verify cross-references and consistency."""
        # Check if default class_priority exists
        if "default_field_config" in self.config:
            default_config = self.config["default_field_config"]
            if "class_priority" in default_config:
                default_priority = default_config["class_priority"]
                if default_priority not in available_priorities:
                    self.errors.append(f"Default class_priority '{default_priority}' not found in class_priority_lists")
    
    def verify(self) -> bool:
        """Run all verification checks sequentially, raising error immediately if any step fails."""
        print(f"Verifying config file: {self.config_path}")
        
        # Load config
        print("Loading configuration...")
        if not self.load_config():
            self.print_results()
            raise ConfigVerificationError(self.errors)
        print("Configuration loaded successfully")
        
        # Run verification checks sequentially with immediate error checking
        print("Verifying structure...")
        self.verify_structure()
        self._check_errors_and_raise("Structure verification")
        print("Structure verification passed")
        
        print(" Verifying available classes...")
        available_classes = self.verify_available_classes()
        self._check_errors_and_raise("Available classes verification")
        print("Available classes verification passed")
        
        print(" Verifying class priority lists...")
        available_priorities = self.verify_class_priority_lists(available_classes)
        self._check_errors_and_raise("Class priority lists verification")
        print("Class priority lists verification passed")
        
        print(" Verifying data not present section...")
        available_empty_keys = set(self.config.get("data_not_present", {}).keys()) if self.config else set()
        self.verify_data_not_present()
        self._check_errors_and_raise("Data not present verification")
        print("Data not present verification passed")
        
        print(" Verifying default field configuration...")
        self.verify_default_field_config()
        self._check_errors_and_raise("Default field configuration verification")
        print("Default field configuration verification passed")
        
        print(" Verifying field configurations...")
        self.verify_fields(available_priorities, available_empty_keys)
        self._check_errors_and_raise("Field configurations verification")
        print("Field configurations verification passed")
        
        print(" Verifying hidden fields...")
        self.verify_hidden_fields()
        self._check_errors_and_raise("Hidden fields verification")
        print("Hidden fields verification passed")
        
        print(" Verifying consistency...")
        self.verify_consistency(available_priorities)
        self._check_errors_and_raise("Consistency verification")
        print("Consistency verification passed")
        
        # If we get here, all verifications passed
        print(" verification checks passed successfully!")
        if self.warnings:
            print(f"Found {len(self.warnings)} warning(s):")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. {warning}")
        
        return True
    
    def print_results(self) -> None:
        """Print verification results."""
        if not self.errors and not self.warnings:
            print("Config verification passed! No issues found.")
            return
        
        if self.errors:
            print(f"Found {len(self.errors)} error(s):")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
        
        if self.warnings:
            print(f"Found {len(self.warnings)} warning(s):")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. {warning}")
        
        print()
        if self.errors:
            print("Config verification FAILED - please fix the errors above.")
        elif self.warnings:
            print("Config verification passed with warnings.")
    def get_config(self) -> Dict:
      if self.config is not None:
        return self.config


      return None
