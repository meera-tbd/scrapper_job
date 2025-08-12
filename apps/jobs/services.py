"""
Job categorization service for automatic job classification.
"""

import re
from typing import Optional


class JobCategorizationService:
    """Service to automatically categorize jobs based on title and description."""
    
    # Define categorization rules
    CATEGORY_KEYWORDS = {
        'technology': [
            'developer', 'engineer', 'programmer', 'software', 'web', 'mobile', 'frontend', 'backend',
            'fullstack', 'devops', 'data scientist', 'data analyst', 'database', 'python', 'java',
            'javascript', 'react', 'node', 'angular', 'vue', 'php', 'ruby', 'c++', 'c#', '.net',
            'ios', 'android', 'ui/ux', 'designer', 'architect', 'technical', 'it support',
            'system admin', 'network', 'security', 'cyber', 'cloud', 'aws', 'azure', 'docker',
            'kubernetes', 'machine learning', 'ai', 'artificial intelligence', 'blockchain'
        ],
        'finance': [
            'accountant', 'financial', 'finance', 'banking', 'investment', 'analyst', 'auditor',
            'bookkeeper', 'treasurer', 'controller', 'cfo', 'financial advisor', 'credit',
            'risk', 'compliance', 'tax', 'payroll', 'budget', 'accounting', 'cpa'
        ],
        'healthcare': [
            'nurse', 'doctor', 'physician', 'medical', 'healthcare', 'health', 'clinical',
            'therapist', 'dentist', 'pharmacist', 'radiologist', 'surgeon', 'veterinarian',
            'physiotherapist', 'psychologist', 'psychiatrist', 'paramedic', 'hospital',
            'clinic', 'patient care', 'medical assistant'
        ],
        'marketing': [
            'marketing', 'digital marketing', 'content', 'social media', 'seo', 'sem', 'ppc',
            'brand', 'advertising', 'campaign', 'communications', 'pr', 'public relations',
            'copywriter', 'content creator', 'graphic designer', 'creative', 'market research',
            'email marketing', 'growth', 'acquisition', 'retention'
        ],
        'sales': [
            'sales', 'business development', 'account manager', 'sales rep', 'sales executive',
            'sales manager', 'sales director', 'business analyst', 'crm', 'lead generation',
            'customer success', 'relationship manager', 'territory', 'quota', 'commission',
            'b2b', 'b2c', 'inside sales', 'outside sales', 'sales coordinator'
        ],
        'hr': [
            'hr', 'human resources', 'recruiter', 'recruitment', 'talent', 'people', 'culture',
            'employee relations', 'benefits', 'compensation', 'training', 'development',
            'organizational', 'workforce', 'staffing', 'onboarding', 'performance',
            'learning and development', 'talent acquisition', 'hr generalist', 'hr manager'
        ],
        'education': [
            'teacher', 'professor', 'instructor', 'educator', 'tutor', 'academic', 'school',
            'university', 'college', 'education', 'curriculum', 'learning', 'training',
            'educational', 'principal', 'administrator', 'librarian', 'counselor',
            'teaching assistant', 'research', 'faculty'
        ],
        'retail': [
            'retail', 'sales assistant', 'cashier', 'store', 'shop', 'merchandising',
            'inventory', 'customer service', 'floor', 'associate', 'supervisor',
            'manager', 'visual merchandising', 'buyer', 'purchasing', 'warehouse',
            'stock', 'product', 'ecommerce', 'online retail'
        ],
        'hospitality': [
            'hotel', 'restaurant', 'hospitality', 'chef', 'cook', 'waiter', 'waitress',
            'bartender', 'barista', 'server', 'host', 'hostess', 'concierge', 'housekeeper',
            'front desk', 'reception', 'guest services', 'food service', 'catering',
            'event', 'tourism', 'travel', 'food and beverage'
        ],
        'construction': [
            'construction', 'builder', 'contractor', 'electrician', 'plumber', 'carpenter',
            'mason', 'roofer', 'welder', 'foreman', 'supervisor', 'project manager',
            'architect', 'civil engineer', 'surveyor', 'heavy equipment', 'trades',
            'apprentice', 'laborer', 'site', 'building', 'infrastructure'
        ],
        'manufacturing': [
            'manufacturing', 'production', 'factory', 'assembly', 'operator', 'technician',
            'quality control', 'quality assurance', 'maintenance', 'mechanical',
            'industrial', 'plant', 'machinery', 'process', 'lean', 'six sigma',
            'supply chain', 'logistics', 'warehouse', 'shipping', 'receiving'
        ],
        'consulting': [
            'consultant', 'consulting', 'advisory', 'advisor', 'strategy', 'management',
            'business consultant', 'freelance', 'independent', 'contractor',
            'professional services', 'expertise', 'specialist', 'practice'
        ],
        'legal': [
            'lawyer', 'attorney', 'legal', 'law', 'paralegal', 'legal assistant',
            'counsel', 'litigation', 'corporate law', 'compliance', 'contracts',
            'intellectual property', 'patent', 'trademark', 'legal advisor',
            'barrister', 'solicitor', 'judicial', 'court'
        ],
        # Extended mappings aligned to Chandler Macleod categories
        'office_support': [
            'office support', 'reception', 'administrator', 'admin', 'clerical', 'secretary', 'pa', 'ea', 'data entry'
        ],
        'drivers_operators': [
            'driver', 'drivers', 'operator', 'forklift', 'excavator', 'dump truck', 'multi skilled', 'crew trainer', 'plant operator'
        ],
        'technical_engineering': [
            'engineer', 'engineering', 'technician', 'mechanical', 'electrical', 'civil', 'draft', 'maintenance', 'reliability', 'asset', 'analyst', 'cost analyst'
        ],
        'production_workers': [
            'production worker', 'production', 'labour', 'laborer', 'warehouse operator', 'mining operators', 'operators', 'process worker'
        ],
        'transport_logistics': [
            'transport', 'logistics', 'fleet', 'freight', 'warehouse', 'supply chain', 'distribution'
        ],
        'mining_resources': [
            'mining', 'resources', 'rio tinto', 'fifo', 'dido', 'bibo', 'mine', 'pit', 'haul truck', 'cat 789', 'excavators', 'dozer'
        ],
        'sales_marketing': [
            'sales', 'marketing', 'customer support', 'customer service', 'account manager', 'representative'
        ],
        'executive': [
            'executive', 'director', 'head of', 'chief', 'c-suite', 'general manager'
        ]
    }
    
    @classmethod
    def categorize_job(cls, title: str, description: str = "") -> str:
        """
        Categorize a job based on its title and description.
        
        Args:
            title: Job title
            description: Job description (optional)
            
        Returns:
            Category string from JOB_CATEGORY_CHOICES
        """
        if not title:
            return 'other'
        
        # Combine title and description for analysis
        text_to_analyze = f"{title} {description}".lower()
        
        # Score each category
        category_scores = {}
        
        for category, keywords in cls.CATEGORY_KEYWORDS.items():
            score = 0
            for keyword in keywords:
                # Count occurrences of each keyword
                keyword_count = len(re.findall(r'\b' + re.escape(keyword.lower()) + r'\b', text_to_analyze))
                # Give more weight to title matches
                title_count = len(re.findall(r'\b' + re.escape(keyword.lower()) + r'\b', title.lower()))
                score += keyword_count + (title_count * 2)  # Title matches count double
            
            category_scores[category] = score
        
        # Find the category with the highest score
        if category_scores:
            best_category = max(category_scores, key=category_scores.get)
            if category_scores[best_category] > 0:
                return best_category
        
        return 'other'

    @classmethod
    def normalize_display_category(cls, raw_text: str) -> str:
        """Return a pretty display label for a raw category string."""
        if not raw_text:
            return 'Other'
        text = raw_text.strip()
        # Title-case but keep ampersands
        text = re.sub(r'\s+', ' ', text.replace('&amp;', '&'))
        return text.title()
    
    @classmethod
    def get_job_keywords(cls, title: str, description: str = "") -> list:
        """
        Extract relevant keywords from job title and description.
        
        Args:
            title: Job title
            description: Job description (optional)
            
        Returns:
            List of relevant keywords
        """
        text = f"{title} {description}".lower()
        found_keywords = []
        
        for category, keywords in cls.CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if re.search(r'\b' + re.escape(keyword.lower()) + r'\b', text):
                    found_keywords.append(keyword)
        
        return list(set(found_keywords))  # Remove duplicates