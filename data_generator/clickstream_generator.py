# data_generator/clickstream_generator.py
import json
import random
import time
import uuid
from datetime import datetime
import time


class ClickstreamGenerator:
    """Generate synthetic clickstream data mimicking user behavior on a website."""
    
    def __init__(self):
        self.users = list(range(1, 1001))  # 1000 users
        self.session_ids = {}
        self.pages = [
            "/", "/products", "/products/category/electronics", "/products/category/clothing",
            "/products/category/home", "/products/item/123", "/products/item/456",
            "/cart", "/checkout", "/payment", "/confirmation", "/account", "/about", "/contact"
        ]
        self.events = ["page_view", "button_click", "add_to_cart", "remove_from_cart", 
                     "checkout_progress", "purchase", "search", "login", "logout", "signup"]
        self.referrers = ["https://google.com", "https://facebook.com", "https://twitter.com", 
                        "https://instagram.com", "direct", "email_campaign", "affiliate"]
        self.devices = ["desktop", "mobile", "tablet"]
        self.browsers = ["chrome", "firefox", "safari", "edge"]
        self.os = ["windows", "macos", "ios", "android", "linux"]
        
    def generate_event(self):
        """Generate a single clickstream event."""
        user_id = random.choice(self.users)
        
        # Create or maintain session for user
        if user_id not in self.session_ids or random.random() < 0.1:  # 10% chance to start new session
            self.session_ids[user_id] = str(uuid.uuid4())
        
        session_id = self.session_ids[user_id]
        timestamp = datetime.now().isoformat()
        page = random.choice(self.pages)
        event_type = random.choice(self.events)
        
        # Generate device info
        device = random.choice(self.devices)
        browser = random.choice(self.browsers)
        os_name = random.choice(self.os)
        
        # More realistic: certain events only happen on certain pages
        if page == "/products/item/123" or page == "/products/item/456":
            if random.random() < 0.3:  # 30% chance on product pages
                event_type = "add_to_cart"
        elif page == "/cart":
            if random.random() < 0.2:  # 20% chance on cart page
                event_type = "checkout_progress"
        elif page == "/payment":
            if random.random() < 0.4:  # 40% chance on payment page
                event_type = "purchase"
        
        # Generate event data
        event = {
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": timestamp,
            "page": page,
            "event_type": event_type,
            "referrer": random.choice(self.referrers),
            "device": {
                "type": device,
                "browser": browser,
                "os": os_name
            },
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": f"Mozilla/5.0 ({os_name}; {device}) Browser/{random.randint(80, 110)}"
        }
        
        # Add event-specific data
        if event_type == "search":
            search_terms = ["laptop", "shoes", "phone", "dress", "headphones"]
            event["search_query"] = random.choice(search_terms)
        elif event_type == "add_to_cart":
            event["product_id"] = random.randint(1, 1000)
            event["product_price"] = round(random.uniform(9.99, 999.99), 2)
            event["quantity"] = random.randint(1, 5)
        elif event_type == "purchase":
            event["order_id"] = str(uuid.uuid4())
            event["total_amount"] = round(random.uniform(19.99, 1999.99), 2)
            
        return event
    
    def generate_batch(self, batch_size=10):
        """Generate a batch of clickstream events."""
        return [self.generate_event() for _ in range(batch_size)]
    
    def generate_continuous(self, interval=1.0, max_events=None):
        """Generate clickstream events continuously with a given interval."""
        count = 0
        while max_events is None or count < max_events:
            yield self.generate_event()
            time.sleep(random.expovariate(1.0/interval))  # Poisson distribution for more realistic timing
            count += 1

# if __name__ == "__main__":
    # Example usage
    # generator = ClickstreamGenerator()
    
    # # Generate 100 events and print them
    # for i, event in enumerate(generator.generate_continuous(interval=0.5, max_events=100)):
    #     print(f"Event {i+1}: {json.dumps(event)}")