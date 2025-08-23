"""
Serializers for the core app API.
"""

from rest_framework import serializers
from .models import Location


class LocationListSerializer(serializers.ModelSerializer):
    """Complete serializer for location listing views with all fields."""
    
    class Meta:
        model = Location
        fields = '__all__'  # Include all fields from the model
        depth = 1  # Include related data with depth
        read_only_fields = ['id', 'created_at', 'updated_at']


class LocationDetailSerializer(serializers.ModelSerializer):
    """Detailed serializer for individual location views."""
    
    class Meta:
        model = Location
        fields = [
            'id', 'name', 'city', 'state', 'country', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']