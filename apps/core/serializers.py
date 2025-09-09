"""
Serializers for core app authentication and common models.
"""

from django.contrib.auth import get_user_model
from rest_framework import serializers


User = get_user_model()


class RegisterSerializer(serializers.ModelSerializer):
    """Serializer to register a new user.

    Exposes username, email, password. Password is write-only.
    """

    password = serializers.CharField(write_only=True, min_length=8)

    class Meta:
        model = User
        fields = ["username", "email", "password", "first_name", "last_name"]

    def validate_username(self, value: str) -> str:
        if User.objects.filter(username__iexact=value).exists():
            raise serializers.ValidationError("Username already exists")
        return value

    def validate_email(self, value: str) -> str:
        if value and User.objects.filter(email__iexact=value).exists():
            raise serializers.ValidationError("Email already exists")
        return value

    def create(self, validated_data):
        password = validated_data.pop("password")
        user = User(**validated_data)
        user.set_password(password)
        user.save()
        return user


class MeSerializer(serializers.ModelSerializer):
    """Serializer for current authenticated user details."""

    class Meta:
        model = User
        fields = ["id", "username", "email", "first_name", "last_name", "is_staff", "is_active"]

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