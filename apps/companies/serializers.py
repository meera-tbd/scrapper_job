"""
Serializers for the companies app API.
"""

from rest_framework import serializers
from .models import Company


class CompanyListSerializer(serializers.ModelSerializer):
    """Clean serializer for company listing views with essential fields only."""
    
    class Meta:
        model = Company
        fields = ['id', 'name', 'slug', 'description', 'created_at', 'updated_at']
        depth = 1  # Include related data with depth
        read_only_fields = ['id', 'slug', 'created_at', 'updated_at']


class CompanyDetailSerializer(serializers.ModelSerializer):
    """Detailed serializer for individual company views."""
    
    class Meta:
        model = Company
        fields = [
            'id', 'name', 'slug', 'description', 'website', 'company_size', 
            'logo', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'slug', 'created_at', 'updated_at']