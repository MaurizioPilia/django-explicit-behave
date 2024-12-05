from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from . import models, serializers


class ChoicesView(ListCreateAPIView):
    serializer_class = serializers.ChoiceSerializer
    queryset = models.Choice.objects.all()


class QuestionsViewSet(ModelViewSet):
    queryset = models.Question.objects.all()
    lookup_url_kwarg = "question_id"

    def get_serializer_class(self):
        # Handle .create() requests
        if self.request.method == "POST":
            return serializers.QuestionDetailPageSerializer
        # Handle .result() requests
        elif self.detail is True and self.request.method == "GET" and self.name == "Result":
            return serializers.QuestionResultPageSerializer
        # Handle .retrieve() requests
        elif self.detail is True and self.request.method == "GET":
            return serializers.QuestionDetailPageSerializer
        return serializers.QuestionListPageSerializer

    @action(detail=True)
    def result(self, request, *args, **kwargs):
        return self.retrieve(self, request, *args, **kwargs)

    @action(methods=["GET", "POST"], detail=True)
    def choices(self, request, *args, **kwargs):
        question = self.get_object()
        if request.method == "GET":
            choices = question.choice_set.all()
            serializer = serializers.QuestionChoiceSerializer(choices, many=True)
            return Response(serializer.data)
        else:
            serializer = serializers.QuestionChoiceSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save(question=question)
                return Response(serializer.data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(methods=["patch"], detail=True)
    def vote(self, request, *args, **kwargs):
        question = self.get_object()
        serializer = serializers.VoteSerializer(data=request.data)
        if serializer.is_valid():
            choice = get_object_or_404(models.Choice, pk=serializer.validated_data["choice_id"], question=question)
            choice.votes += 1
            choice.save()
            return Response("Voted")
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
