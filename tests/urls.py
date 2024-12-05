from django.urls import path, include

urlpatterns = [
    path('polls/', include('polls.urls')),
]