import java.util.Arrays;

/**
 * 归并排序
 */
public class MergeSort {

    public static void mergeSort(int[] arr, int left, int right) {
        if (left < right) {
            int middle = (left + right) / 2;
            mergeSort(arr, left, middle);
            mergeSort(arr, middle + 1, right);
            merge(arr, left, middle, right);
        }
    }

    public static void merge(int[] arr, int left, int middle, int right) {
        int[] tmpArr = new int[arr.length];// 临时数组
        int tmp = left; // 缓存左数组第一个元素的索引
        int third = left;// third 记录临时数组的索引
        int mid = middle + 1;  // 右数组第一个元素索引

        while (left <= middle && mid <= right) {
            //从两个数组中取出最小的放入到临时数组中
            if (arr[left] <= arr[mid]) {
                tmpArr[third++] = arr[left++];
            } else {
                tmpArr[third++] = arr[mid++];
            }
        }
        // 剩余部分依次放入临时数组（实际上两个while只会执行其中一个）
        while (mid <= right) {
            tmpArr[third++] = arr[mid++];
        }
        while (left <= middle) {
            tmpArr[third++] = arr[left++];
        }
        // 将临时数组中的内容拷贝回原数组中
        // （原left-right范围的内容被复制回原数组）
        while (tmp <= right) {
            arr[tmp] = tmpArr[tmp];
            tmp++;
        }
    }

    public static void main(String[] args) {
        int[] arr = {1, 3, 2, 5, 4, 2, 6, 5, 3};
        mergeSort(arr, 0, arr.length - 1);
        System.out.println(Arrays.toString(arr));
    }
}
