/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package priority

import (
	"encoding/json"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	resourcehelpers "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/resources"
	vpa_utils "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

// oomRecencyWindow bounds how long after an OOM the TargetHigherThanRequestsAndOOM
// requirement keeps treating it as actionable. It stops an old OOM record on a
// long-lived, never-recreated pod from authorizing evictions forever. It is a
// coarse bound; it deliberately does not track the exact evict-after-oom threshold.
const oomRecencyWindow = 10 * time.Minute

const cachedOOMAnnotation = "vpa-updater.autoscaling.k8s.io/oom-observations"

type cachedOOMObservation struct {
	FinishedAt         time.Time `json:"finishedAt"`
	MemoryRequestBytes int64     `json:"memoryRequestBytes"`
}

// NewScalingDirectionPodEvictionAdmission creates a PodEvictionAdmission object.
// It admits Pods for eviction based on the scaling direction, i.e. if a resource is scaled up (recommendation > requests) or scaled down (recommendation < requests).
// It also supports >= and <= relations.
func NewScalingDirectionPodEvictionAdmission() PodEvictionAdmission {
	return &scalingDirectionPodEvictionAdmission{}
}

type scalingDirectionPodEvictionAdmission struct {
	EvictionRequirements map[*corev1.Pod][]*vpa_types.EvictionRequirement
}

// Admit admits a Pod for eviction in one of three cases
// * no EvictionRequirement exists for this Pod
// * no Resource requests are set for at least one Container in this Pod
// * all EvictionRequirements are evaluated to 'true' for at least one Container in this Pod
func (s *scalingDirectionPodEvictionAdmission) Admit(pod *corev1.Pod, resources *vpa_types.RecommendedPodResources) bool {
	podEvictionRequirements, found := s.EvictionRequirements[pod]
	if !found {
		return true
	}
	oomContainers := containersWithActionableOOM(pod, time.Now())
	for _, container := range pod.Spec.Containers {
		recommendedResources := vpa_utils.GetRecommendationForContainer(container.Name, resources)
		// if a container doesn't have a recommendation, the VPA has set `.containerPolicy.mode: off` for this container,
		// so we can skip this container
		if recommendedResources == nil {
			continue
		}
		containerRequests, _ := resourcehelpers.ContainerRequestsAndLimits(container.Name, pod)
		if s.admitContainer(containerRequests, recommendedResources, podEvictionRequirements, oomContainers.Has(container.Name)) {
			return true
		}
	}
	return false
}

func (s *scalingDirectionPodEvictionAdmission) admitContainer(containerRequests corev1.ResourceList, recommendedResources *vpa_types.RecommendedContainerResources, podEvictionRequirements []*vpa_types.EvictionRequirement, containerHasActionableOOM bool) bool {
	_, foundCPURequests := containerRequests[corev1.ResourceCPU]
	if !foundCPURequests {
		return true
	}
	_, foundMemoryRequests := containerRequests[corev1.ResourceMemory]
	if !foundMemoryRequests {
		return true
	}
	return s.checkEvictionRequirementsForContainer(containerRequests, recommendedResources.Target, podEvictionRequirements, containerHasActionableOOM)
}

func (s *scalingDirectionPodEvictionAdmission) checkEvictionRequirementsForContainer(requestedResources corev1.ResourceList, recommendedResources corev1.ResourceList, evictionRequirements []*vpa_types.EvictionRequirement, containerHasActionableOOM bool) bool {
	for _, requirement := range evictionRequirements {
		var resultsForResources = []bool{}
		for _, resource := range requirement.Resources {
			currentlyRequestedValue := requestedResources[resource]
			recommendedValue := recommendedResources[resource]
			resultsForResources = append(resultsForResources, s.checkChangeRequirement(currentlyRequestedValue.MilliValue(), recommendedValue.MilliValue(), requirement.ChangeRequirement, containerHasActionableOOM))
		}
		// *All* EvictionRequirements need to be evaluated to `true`. Each requirement passes if *at least one* of its resources satisfies changeRequirement. So if there's a single EvictionRequirement which evaluates to `false` because none of its resources satisfies changeRequirement, we can exit here and don't admit the Container
		if !slices.Contains(resultsForResources, true) {
			return false
		}
	}
	return true
}

func (*scalingDirectionPodEvictionAdmission) checkChangeRequirement(currentRequests int64, recommendation int64, changeRequirement vpa_types.EvictionChangeRequirement, containerHasActionableOOM bool) bool {
	switch changeRequirement {
	case vpa_types.TargetHigherThanRequests:
		return recommendation > currentRequests
	case vpa_types.TargetLowerThanRequests:
		return recommendation < currentRequests
	case vpa_types.TargetHigherThanRequestsAndOOM:
		return recommendation > currentRequests && containerHasActionableOOM
	default:
		return false
	}
}

func containersWithActionableOOM(pod *corev1.Pod, now time.Time) sets.Set[string] {
	result := sets.New[string]()
	for containerName, observation := range cachedOOMObservations(pod) {
		if now.Sub(observation.FinishedAt) >= oomRecencyWindow {
			continue
		}
		requests, _ := resourcehelpers.ContainerRequestsAndLimits(containerName, pod)
		memory, found := requests[corev1.ResourceMemory]
		if !found || memory.Value() != observation.MemoryRequestBytes {
			continue
		}
		result.Insert(containerName)
	}
	return result
}

func oomTermination(cs *corev1.ContainerStatus) *corev1.ContainerStateTerminated {
	if t := cs.State.Terminated; t != nil && t.Reason == "OOMKilled" {
		return t
	}
	if t := cs.LastTerminationState.Terminated; t != nil && t.Reason == "OOMKilled" {
		return t
	}
	return nil
}

func cachedOOMObservations(pod *corev1.Pod) map[string]cachedOOMObservation {
	result := make(map[string]cachedOOMObservation)
	value, found := pod.Annotations[cachedOOMAnnotation]
	if !found {
		return result
	}
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		return make(map[string]cachedOOMObservation)
	}
	return result
}

func setCachedOOMObservations(pod *corev1.Pod, observations map[string]cachedOOMObservation) {
	if pod.Annotations != nil {
		delete(pod.Annotations, cachedOOMAnnotation)
	}
	if len(observations) == 0 {
		return
	}
	value, err := json.Marshal(observations)
	if err != nil {
		return
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[cachedOOMAnnotation] = string(value)
}

func findContainerStatus(name string, statuses []corev1.ContainerStatus) *corev1.ContainerStatus {
	for i := range statuses {
		if statuses[i].Name == name {
			return &statuses[i]
		}
	}
	return nil
}

func recordCachedOOM(observations map[string]cachedOOMObservation, pod *corev1.Pod, containerName string, finishedAt time.Time) {
	requests, _ := resourcehelpers.ContainerRequestsAndLimits(containerName, pod)
	memory, found := requests[corev1.ResourceMemory]
	if !found {
		return
	}
	observations[containerName] = cachedOOMObservation{
		FinishedAt:         finishedAt,
		MemoryRequestBytes: memory.Value(),
	}
}

func UpdateCachedPodOOMs(oldPod, newPod *corev1.Pod, now time.Time) {
	observations := make(map[string]cachedOOMObservation)
	if oldPod != nil {
		observations = cachedOOMObservations(oldPod)
		for i := range newPod.Status.ContainerStatuses {
			status := &newPod.Status.ContainerStatuses[i]
			oldStatus := findContainerStatus(status.Name, oldPod.Status.ContainerStatuses)
			if oldStatus == nil {
				continue
			}
			restarted := status.RestartCount > oldStatus.RestartCount
			newOOM := status.State.Terminated != nil &&
				status.State.Terminated.Reason == "OOMKilled" &&
				(oldStatus.State.Terminated == nil || restarted)
			previousOOM := status.State.Running != nil &&
				oldStatus.State.Running != nil &&
				restarted &&
				status.LastTerminationState.Terminated != nil &&
				status.LastTerminationState.Terminated.Reason == "OOMKilled"
			switch {
			case newOOM:
				recordCachedOOM(observations, newPod, status.Name, status.State.Terminated.FinishedAt.Time)
			case previousOOM:
				recordCachedOOM(observations, oldPod, status.Name, status.LastTerminationState.Terminated.FinishedAt.Time)
			}
		}
	} else {
		for i := range newPod.Status.ContainerStatuses {
			status := &newPod.Status.ContainerStatuses[i]
			if terminated := oomTermination(status); terminated != nil {
				recordCachedOOM(observations, newPod, status.Name, terminated.FinishedAt.Time)
			}
		}
	}

	for containerName, observation := range observations {
		requests, _ := resourcehelpers.ContainerRequestsAndLimits(containerName, newPod)
		memory, found := requests[corev1.ResourceMemory]
		if !found || memory.Value() != observation.MemoryRequestBytes || now.Sub(observation.FinishedAt) >= oomRecencyWindow {
			delete(observations, containerName)
		}
	}
	setCachedOOMObservations(newPod, observations)
}

// LoopInit initializes the object by creating a map holding all applicable EvictionRequirements for each Pod.
// The map is re-created on every call, to ensure that any changes to a VPA's EvictionRequirements are picked up and not leak any EvictionRequirements for no longer existing Pods.
func (s *scalingDirectionPodEvictionAdmission) LoopInit(_ []*corev1.Pod, vpaControlledPods map[*vpa_types.VerticalPodAutoscaler][]*corev1.Pod) {
	s.EvictionRequirements = make(map[*corev1.Pod][]*vpa_types.EvictionRequirement)
	for vpa, pods := range vpaControlledPods {
		for _, pod := range pods {
			// When UpdatePolicy is not specified, the default policy will be followed, and the EvictionRequirements field will be nil
			if vpa.Spec.UpdatePolicy == nil {
				continue
			}
			s.EvictionRequirements[pod] = vpa.Spec.UpdatePolicy.EvictionRequirements
		}
	}
}

func (s *scalingDirectionPodEvictionAdmission) CleanUp() {
	s.EvictionRequirements = make(map[*corev1.Pod][]*vpa_types.EvictionRequirement)
}
